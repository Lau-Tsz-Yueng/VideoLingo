"""
Lightweight S3 poller to trigger VideoLingo CLI for new HLS playlists.

Workflow:
1) List `.m3u8` files under INPUT_BUCKET/INPUT_PREFIX.
2) For each unseen key (no marker in OUTPUT_BUCKET/MARKER_PREFIX), generate a presigned URL.
3) Run `cli_run.py` with that URL, upload outputs to OUTPUT_BUCKET/OUTPUT_ROOT/<job_id>/...
4) Write a marker JSON to OUTPUT_BUCKET/MARKER_PREFIX/<job_id>.json so the same input
   key is skipped on subsequent polls.

This is a minimal substitute for Mongo/change-stream when you just want to drive
VideoLingo from S3. Run it in a loop or via cron/systemd as you prefer.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

log = logging.getLogger("videolingo.s3_poller")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


@dataclass
class Settings:
    input_bucket: str
    input_prefix: str
    output_bucket: str
    output_root: str
    marker_prefix: str
    staging_dir: Path
    source_lang: Optional[str]
    target_lang: Optional[str]
    hls_segment: int
    poll_interval: int
    retry_failed: bool
    max_retries: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            input_bucket=_env("VL_INPUT_BUCKET"),
            input_prefix=os.getenv("VL_INPUT_PREFIX", "").strip(),
            output_bucket=_env("VL_OUTPUT_BUCKET"),
            output_root=os.getenv("VL_OUTPUT_ROOT", "videolingo/jobs"),
            marker_prefix=os.getenv("VL_MARKER_PREFIX", "videolingo/markers"),
            staging_dir=Path(os.getenv("VL_STAGING_DIR", "batch/input/hls_staging")).resolve(),
            source_lang=os.getenv("VL_SOURCE_LANG"),
            target_lang=os.getenv("VL_TARGET_LANG"),
            hls_segment=int(os.getenv("VL_HLS_SEGMENT", "6")),
            poll_interval=int(os.getenv("VL_POLL_INTERVAL", "30")),
            retry_failed=os.getenv("VL_RETRY_FAILED", "0") == "1",
            max_retries=int(os.getenv("VL_MAX_RETRIES", "3")),
        )


def _s3_client() -> boto3.client:
    return boto3.client("s3")


def _list_m3u8_keys(s3, bucket: str, prefix: str) -> Iterable[str]:
    continuation = None
    total = 0
    while True:
        params = {"Bucket": bucket, "Prefix": prefix} if prefix else {"Bucket": bucket}
        if continuation:
            params["ContinuationToken"] = continuation
        resp = s3.list_objects_v2(**params)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".m3u8"):
                total += 1
                yield key
        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")
    log.info("Listed %s .m3u8 keys under %s/%s", total, bucket, prefix or "")


def _job_id_from_key(key: str) -> str:
    safe = key.replace("/", "_").replace(" ", "_")
    return safe.rsplit(".m3u8", 1)[0]


def _marker_key(settings: Settings, job_id: str) -> str:
    return f"{settings.marker_prefix.rstrip('/')}/{job_id}.json"


def _get_marker(s3, settings: Settings, job_id: str) -> Optional[Dict]:
    try:
        obj = s3.get_object(Bucket=settings.output_bucket, Key=_marker_key(settings, job_id))
        body = obj["Body"].read().decode("utf-8")
        return json.loads(body)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False
        raise


def _write_marker(s3, settings: Settings, job_id: str, payload: Dict):
    payload = dict(payload)
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(
        Bucket=settings.output_bucket,
        Key=_marker_key(settings, job_id),
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def _presign_input(s3, settings: Settings, key: str) -> str:
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.input_bucket, "Key": key},
        ExpiresIn=3600,
    )


def _output_prefix(settings: Settings, job_id: str) -> str:
    base = settings.output_root.rstrip("/")
    return f"s3://{settings.output_bucket}/{base}/{job_id}"


def _download_hls_to_local(s3, settings: Settings, key: str, job_id: str) -> Path:
    """
    Download playlist + segments under the same prefix to a local staging dir.
    """
    playlist_prefix = key.rsplit("/", 1)[0] + "/"
    dest_dir = settings.staging_dir / job_id
    dest_dir.mkdir(parents=True, exist_ok=True)

    continuation = None
    count = 0
    while True:
        params = {"Bucket": settings.input_bucket, "Prefix": playlist_prefix}
        if continuation:
            params["ContinuationToken"] = continuation
        resp = s3.list_objects_v2(**params)
        for obj in resp.get("Contents", []):
            src_key = obj["Key"]
            rel = src_key[len(playlist_prefix) :]
            if not rel:
                continue
            dest_path = dest_dir / rel
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(settings.input_bucket, src_key, str(dest_path))
            count += 1
        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")

    log.info("Downloaded %s objects from %s to %s", count, playlist_prefix, dest_dir)
    return dest_dir / Path(key).name


def _build_cli_command(url: str, job_id: str, settings: Settings) -> List[str]:
    cmd = [
        "python",
        "cli_run.py",
        "--video_path",
        url,
        "--job_id",
        job_id,
        "--hls_output",
        "--hls_segment",
        str(settings.hls_segment),
        "--output_s3_prefix",
        _output_prefix(settings, job_id),
    ]
    if settings.source_lang:
        cmd.extend(["--source_lang", settings.source_lang])
    if settings.target_lang:
        cmd.extend(["--target_lang", settings.target_lang])
    return cmd


def process_key(s3, key: str, settings: Settings):
    job_id = _job_id_from_key(key)
    marker = _get_marker(s3, settings, job_id)
    if marker:
        status = marker.get("status")
        retries = int(marker.get("retries", 0))
        if status == "completed":
            log.info("Skip %s (already completed)", key)
            return
        if status == "failed":
            if settings.retry_failed and retries < settings.max_retries:
                log.info("Retrying failed job %s (attempt %s/%s)", job_id, retries + 1, settings.max_retries)
            else:
                log.info("Skip %s (failed %s times, max_retries=%s)", key, retries, settings.max_retries)
                return
        else:
            log.info("Skip %s (marker status=%s)", key, status)
            return

    log.info("Processing new HLS: %s (job_id=%s)", key, job_id)
    # Download the whole HLS prefix locally to avoid per-segment presign issues
    local_playlist = _download_hls_to_local(s3, settings, key, job_id)
    cmd = _build_cli_command(str(local_playlist), job_id, settings)

    try:
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)
        _write_marker(
            s3,
            settings,
            job_id,
            {
                "status": "completed",
                "input_key": key,
                "output_prefix": _output_prefix(settings, job_id),
                "retries": int(marker.get("retries", 0)) + 1 if marker else 0,
            },
        )
        log.info("Completed job %s", job_id)
    except subprocess.CalledProcessError as exc:
        _write_marker(
            s3,
            settings,
            job_id,
            {
                "status": "failed",
                "input_key": key,
                "error": str(exc),
                "retries": int(marker.get("retries", 0)) + 1 if marker else 1,
            },
        )
        log.error("Job %s failed: %s", job_id, exc)


def poll_loop():
    settings = Settings.from_env()
    s3 = _s3_client()
    log.info(
        "S3 poller started: input=%s/%s output=%s/%s markers=%s interval=%ss retry_failed=%s max_retries=%s",
        settings.input_bucket,
        settings.input_prefix or "",
        settings.output_bucket,
        settings.output_root,
        settings.marker_prefix,
        settings.poll_interval,
        settings.retry_failed,
        settings.max_retries,
    )
    while True:
        try:
            found = False
            for key in _list_m3u8_keys(s3, settings.input_bucket, settings.input_prefix):
                found = True
                process_key(s3, key, settings)
            if not found:
                log.info("No new .m3u8 found; sleeping %ss", settings.poll_interval)
        except Exception as exc:
            log.exception("Poller error: %s", exc)
        time.sleep(settings.poll_interval)


if __name__ == "__main__":
    poll_loop()
