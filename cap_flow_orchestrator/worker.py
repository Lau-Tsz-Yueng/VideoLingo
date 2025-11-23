"""
Minimal cap-flow orchestrator skeleton for VideoLingo.

- Watches MongoDB change stream to enqueue VideoLingo jobs.
- Dispatches pending jobs to Runpod (VideoLingo container) with cli_run.py.
- Polls Runpod for completion, reads manifest.json from S3, and updates the post.

This is intentionally lightweight and avoids hard-coding business details; adapt
the field names and Runpod API payloads to your setup.
"""

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

CONFIG_PATH = os.getenv("VL_CONFIG_FILE", ".env")
if CONFIG_PATH:
    env_path = Path(CONFIG_PATH)
    if env_path.exists():
        load_dotenv(env_path)
        log.info("Loaded environment variables from %s", env_path)


def _env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _env_list(name: str) -> set:
    raw = os.getenv(name, "")
    return {x.strip() for x in raw.split(",") if x.strip()}


def _parse_s3_prefix(prefix: str) -> Tuple[str, str]:
    parsed = urlparse(prefix)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 prefix: {prefix}")
    return parsed.netloc, parsed.path.lstrip("/")


@dataclass
class Settings:
    mongo_uri: str
    posts_collection: str
    jobs_collection: str
    whitelist_user_ids: set
    pod_run_url: str
    pod_timeout: int
    output_bucket: str
    output_prefix: str
    source_lang: Optional[str]
    target_lang: Optional[str]
    enable_dubbing: bool
    hls_segment: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mongo_uri=_env("MONGO_URI", required=True),
            posts_collection=_env("VL_POSTS_COLLECTION", "posts"),
            jobs_collection=_env("VL_JOBS_COLLECTION", "videolingo_jobs"),
            whitelist_user_ids=_env_list("VL_WHITELIST_USER_IDS"),
            pod_run_url=_env("VL_POD_RUN_URL", required=True),
            pod_timeout=int(_env("VL_POD_TIMEOUT", "7200")),
            output_bucket=_env("VL_OUTPUT_BUCKET", required=True),
            output_prefix=_env("VL_OUTPUT_PREFIX", "jobs"),
            source_lang=_env("VL_SOURCE_LANG"),
            target_lang=_env("VL_TARGET_LANG"),
            enable_dubbing=_env("VL_ENABLE_DUBBING", "0") == "1",
            hls_segment=int(_env("VL_HLS_SEGMENT", "6")),
        )


def _build_output_prefix(settings: Settings, job_id: str) -> str:
    prefix = settings.output_prefix.rstrip("/")
    suffix = f"{prefix}/{job_id}" if prefix else job_id
    return f"s3://{settings.output_bucket}/{suffix}"


def _extract_s3_input_key(post: Dict[str, Any]) -> Optional[str]:
    video = post.get("video") or {}
    key = video.get("hls_s3_key") or video.get("hls_input_key")
    if key:
        return key
    playlist = video.get("hls_playlist_url")
    if playlist:
        parsed = urlparse(playlist)
        if parsed.scheme == "s3" and parsed.netloc:
            return parsed.path.lstrip("/")
    return None


def _should_trigger(post: Dict[str, Any], whitelist: set) -> bool:
    video = post.get("video") or {}
    has_subs = post.get("has_subtitles", False)
    s3_key = _extract_s3_input_key(post)
    user_id = str(post.get("user_id") or "")

    if whitelist and user_id not in whitelist:
        return False
    if not s3_key:
        return False
    if has_subs:
        return False
    return True


def _job_exists(jobs: Collection, post_id: Any) -> bool:
    return jobs.count_documents({"post_id": post_id, "status": {"$in": ["pending", "starting", "running"]}}) > 0


def _create_job_doc(post: Dict[str, Any], settings: Settings, s3_input_key: str) -> Dict[str, Any]:
    job_id = str(uuid.uuid4())
    output_prefix = _build_output_prefix(settings, job_id)
    return {
        "job_id": job_id,
        "post_id": post["_id"],
        "s3_input_key": s3_input_key,
        "output_s3_prefix": output_prefix,
        "status": "pending",
        "source_lang": settings.source_lang,
        "target_lang": settings.target_lang,
        "enable_dubbing": settings.enable_dubbing,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }


def _build_request_payload(job: Dict[str, Any], settings: Settings) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "job_id": job["job_id"],
        "s3_input_key": job["s3_input_key"],
        "s3_output_prefix": job["output_s3_prefix"],
        "hls_segment": settings.hls_segment,
        "dubbing": job.get("enable_dubbing", settings.enable_dubbing),
    }
    if settings.source_lang:
        payload["source_lang"] = settings.source_lang
    if settings.target_lang:
        payload["target_lang"] = settings.target_lang
    return payload


def _load_manifest(output_prefix: str) -> Dict[str, Any]:
    bucket, key_prefix = _parse_s3_prefix(output_prefix)
    manifest_key = f"{key_prefix}/manifest.json" if key_prefix else "manifest.json"
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=manifest_key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _update_post_with_manifest(posts: Collection, post_id: Any, manifest: Dict[str, Any]):
    files = manifest.get("files", {})
    update_doc = {
        "has_subtitles": True,
        "subtitle_vtt_url": files.get("vtt"),
        "subtitle_hls_master": files.get("hls_master"),
        "subtitle_hls_playlist": files.get("hls_playlist"),
        "subtitle_srt_url": files.get("srt"),
        "subtitle_video_url": files.get("mp4_with_subs"),
        "updated_at": datetime.utcnow(),
    }
    posts.update_one({"_id": post_id}, {"$set": update_doc})


def watch_posts_for_jobs(posts: Collection, jobs: Collection, settings: Settings):
    """
    Change stream watcher: inserts a pending job when a new eligible post appears.
    """
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update"]}}}]
    while True:
        try:
            with posts.watch(pipeline, full_document="updateLookup") as stream:
                for change in stream:
                    post = change.get("fullDocument")
                    if not post:
                        continue
                    if not _should_trigger(post, settings.whitelist_user_ids):
                        continue
                    input_key = _extract_s3_input_key(post)
                    if not input_key:
                        continue
                    if _job_exists(jobs, post["_id"]):
                        continue
                    job_doc = _create_job_doc(post, settings, input_key)
                    jobs.insert_one(job_doc)
                    log.info("Enqueued job %s for post %s", job_doc["job_id"], post["_id"])
        except PyMongoError as exc:
            log.warning("Change stream dropped (%s); retrying in 5s", exc)
            time.sleep(5)


def dispatch_pending_jobs(jobs: Collection, posts: Collection, settings: Settings):
    job = jobs.find_one_and_update(
        {"status": "pending"},
        {"$set": {"status": "running", "updated_at": datetime.utcnow()}},
    )
    if not job:
        return

    payload = _build_request_payload(job, settings)
    try:
        resp = requests.post(settings.pod_run_url, json=payload, timeout=settings.pod_timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            raise RuntimeError(f"Pod returned failure: {data}")
    except Exception as exc:
        log.exception("VideoLingo job %s failed to start: %s", job["job_id"], exc)
        jobs.update_one(
            {"_id": job["_id"]},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": datetime.utcnow()}},
        )
        return

    try:
        manifest = _load_manifest(job["output_s3_prefix"])
        jobs.update_one(
            {"_id": job["_id"]},
            {
                "$set": {
                    "status": "completed",
                    "manifest": manifest,
                    "pod_response": data,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        _update_post_with_manifest(posts, job["post_id"], manifest)
        log.info("Job %s completed and manifest stored", job["job_id"])
    except Exception as exc:
        jobs.update_one(
            {"_id": job["_id"]},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": datetime.utcnow()}},
        )
        log.exception("Failed to finalize job %s: %s", job["job_id"], exc)


def main():
    settings = Settings.from_env()
    client = MongoClient(settings.mongo_uri)
    db = client.get_default_database()
    posts = db[settings.posts_collection]
    jobs = db[settings.jobs_collection]

    log.info("Starting VideoLingo orchestrator: posts=%s jobs=%s", settings.posts_collection, settings.jobs_collection)

    # Spawn change-stream watcher in a background thread.
    import threading

    watcher_thread = threading.Thread(
        target=watch_posts_for_jobs, args=(posts, jobs, settings), daemon=True
    )
    watcher_thread.start()

    while True:
        dispatch_pending_jobs(jobs, posts, settings)
        time.sleep(5)


if __name__ == "__main__":
    main()
