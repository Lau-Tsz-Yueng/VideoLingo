"""
Runpod serverless handler for VideoLingo.

Expected input payload (Runpod /run):
{
  "input": {
    "hls_url": "https://example.com/master.m3u8",
    "output_s3_prefix": "s3://capflow-videolingo/job-123",
    "source_lang": "en",            # optional, defaults from env
    "target_lang": "fr",            # optional, defaults from env
    "job_id": "job-123",            # optional, auto-generated if missing
    "hls_segment": 6,               # optional
    "dubbing": false                # optional
  }
}
"""

import os
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict

import runpod


REPO_ROOT = Path(__file__).resolve().parent


def _env_default(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def _build_cli_cmd(payload: Dict[str, Any]) -> list[str]:
    hls_url = payload["hls_url"]
    output_s3_prefix = payload["output_s3_prefix"]
    job_id = payload.get("job_id") or f"job-{uuid.uuid4().hex[:8]}"

    source_lang = payload.get("source_lang") or _env_default("VL_DEFAULT_SOURCE_LANG")
    target_lang = payload.get("target_lang") or _env_default("VL_DEFAULT_TARGET_LANG")
    dubbing = payload.get("dubbing", False)
    hls_segment = payload.get("hls_segment") or _env_default("VL_HLS_SEGMENT", "6")

    cmd = [
        "python",
        "cli_run.py",
        "--video_path",
        hls_url,
        "--hls_output",
        "--job_id",
        job_id,
        "--output_s3_prefix",
        output_s3_prefix,
        "--hls_segment",
        str(hls_segment),
    ]
    if source_lang:
        cmd.extend(["--source_lang", source_lang])
    if target_lang:
        cmd.extend(["--target_lang", target_lang])
    if dubbing:
        cmd.append("--dubbing")
    return cmd, job_id


def _run_cli(cmd: list[str]):
    # Run VideoLingo CLI inside repo root
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result


def handler(job: Dict[str, Any]):
    payload = job.get("input") or {}
    if "hls_url" not in payload or "output_s3_prefix" not in payload:
        return {"error": "hls_url and output_s3_prefix are required"}

    cmd, job_id = _build_cli_cmd(payload)
    result = _run_cli(cmd)

    if result.returncode != 0:
        return {
            "job_id": job_id,
            "error": "VideoLingo failed",
            "stdout": result.stdout[-4000:],  # cap the size
            "stderr": result.stderr[-4000:],
        }

    return {
        "job_id": job_id,
        "output_s3_prefix": payload["output_s3_prefix"],
        "stdout": result.stdout[-4000:],
    }


runpod.serverless.start({"handler": handler})
