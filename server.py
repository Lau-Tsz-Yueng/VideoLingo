"""
Minimal FastAPI wrapper that exposes VideoLingo's CLI as an HTTP job runner.

The server accepts JSON payloads describing an input HLS key in S3, invokes
`cli_run.py`, and uploads the resulting subtitles/manifest back to S3.

Environment variables:
    VL_INPUT_BUCKET      - S3 bucket containing input HLS playlists (required).
    VL_OUTPUT_BUCKET     - Default bucket for outputs (required unless the
                           request provides an absolute s3:// prefix).
    VL_PROJECT_ROOT      - Where cli_run.py lives (default: repo root).
    VL_JOB_TIMEOUT       - Optional seconds for the CLI subprocess timeout.

Standard AWS credentials (`AWS_ACCESS_KEY_ID`, etc.) must be present so boto3
can read inputs and write outputs.
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator

log = logging.getLogger("videolingo.server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI(title="VideoLingo Pod Server", version="0.1.0")

PROJECT_ROOT = Path(os.getenv("VL_PROJECT_ROOT", Path(__file__).resolve().parent))
ENV_FILE = PROJECT_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

INPUT_BUCKET = os.getenv("VL_INPUT_BUCKET")
OUTPUT_BUCKET = os.getenv("VL_OUTPUT_BUCKET")
CLI_PATH = PROJECT_ROOT / "cli_run.py"
JOB_TIMEOUT = int(os.getenv("VL_JOB_TIMEOUT", "0")) or None

if not CLI_PATH.exists():
    raise RuntimeError(f"cli_run.py not found at {CLI_PATH}")

if not INPUT_BUCKET:
    raise RuntimeError("VL_INPUT_BUCKET is required for server to know input bucket")
if not OUTPUT_BUCKET:
    raise RuntimeError("VL_OUTPUT_BUCKET is required (can be overridden per request via absolute prefix)")

s3_client = boto3.client("s3")


class JobRequest(BaseModel):
    job_id: str
    s3_input_key: str
    s3_output_prefix: str
    source_lang: Optional[str] = None
    target_lang: Optional[str] = None
    hls_segment: int = Field(default=6, ge=2, le=30)
    dubbing: bool = False

    @validator("s3_output_prefix")
    def _normalize_prefix(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("s3_output_prefix cannot be empty")
        if normalized.startswith("s3://"):
            return normalized.rstrip("/")
        return f"s3://{OUTPUT_BUCKET}/{normalized.lstrip('/')}"

    @property
    def output_bucket(self) -> str:
        return self.s3_output_prefix.split("/", 3)[2]


def _presign_input(key: str) -> str:
    log.info("Generating presigned URL for %s/%s", INPUT_BUCKET, key)
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": INPUT_BUCKET, "Key": key},
        ExpiresIn=3600,
    )


def _build_cli_command(job: JobRequest, video_path: str) -> list[str]:
    cmd = [
        "python",
        str(CLI_PATH),
        "--video_path",
        video_path,
        "--job_id",
        job.job_id,
        "--hls_output",
        "--hls_segment",
        str(job.hls_segment),
        "--output_s3_prefix",
        job.s3_output_prefix,
    ]
    if job.source_lang:
        cmd.extend(["--source_lang", job.source_lang])
    if job.target_lang:
        cmd.extend(["--target_lang", job.target_lang])
    if job.dubbing:
        cmd.append("--dubbing")
    return cmd


@app.get("/healthz")
def healthcheck():
    return {"status": "ok"}


@app.post("/run")
def run_videolingo_job(job: JobRequest):
    if not job.s3_input_key:
        raise HTTPException(status_code=400, detail="s3_input_key is required")

    presigned_url = _presign_input(job.s3_input_key)
    cmd = _build_cli_command(job, presigned_url)
    log.info("Starting VideoLingo job %s (output=%s)", job.job_id, job.s3_output_prefix)

    try:
        subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True, timeout=JOB_TIMEOUT)
    except subprocess.TimeoutExpired:
        log.exception("Job %s timed out", job.job_id)
        raise HTTPException(status_code=504, detail="VideoLingo job timed out")
    except subprocess.CalledProcessError as exc:
        log.exception("Job %s failed: %s", job.job_id, exc)
        raise HTTPException(status_code=500, detail=f"VideoLingo CLI failed: {exc}")

    log.info("Job %s finished successfully", job.job_id)
    return {"status": "success", "job_id": job.job_id, "output_s3_prefix": job.s3_output_prefix}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
