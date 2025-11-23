# Cap-Flow VideoLingo Orchestrator

This worker glues the `cap-flow` MongoDB to the long-lived VideoLingo Pod (the A5000 box that runs `projects/VideoLingo/server.py`). Instead of spinning new Runpod jobs, it simply watches Mongo for new posts, POSTs to the Pod, and writes the resulting subtitle URLs back into the database.

- **Change stream** watches MongoDB for new/updated posts that have an HLS key in S3 but still lack subtitles.
- **Dispatcher** grabs a pending job, calls the Pod's `POST /run`, and blocks until the Pod finishes running `cli_run.py`.
- **Finisher** loads `manifest.json` from S3 (written by VideoLingo) and patches both the `videolingo_jobs` document and the original `posts` record.

## Quickstart

```bash
cd projects/cap_flow_orchestrator
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # 根据需要修改连接串 / 桶 / Pod URL
python worker.py
```

> `worker.py` 会自动加载 `.env`（或设置 `VL_CONFIG_FILE=/path/to/file` 指向其他配置），所有 Runpod/POD/S3/Mongo 配置集中写在这里即可。

## Environment Variables

| Var | Purpose |
| --- | --- |
| `VL_CONFIG_FILE` | Optional path to a `.env` file (default `.env`). |
| `MONGO_URI` | MongoDB connection string. |
| `VL_POSTS_COLLECTION` | Posts collection name (default `posts`). |
| `VL_JOBS_COLLECTION` | Jobs collection name (default `videolingo_jobs`). |
| `VL_WHITELIST_USER_IDS` | Optional comma-separated user id allowlist. |
| `VL_POD_RUN_URL` | Pod HTTP endpoint, e.g. `http://10.0.0.5:8000/run`. |
| `VL_POD_TIMEOUT` | Seconds to wait for the Pod response (default `7200`). |
| `VL_OUTPUT_BUCKET` | Output S3 bucket (e.g. `cw-dev-hls-output-bucket`). |
| `VL_OUTPUT_PREFIX` | Prefix inside the output bucket (e.g. `videolingo/jobs`). |
| `VL_SOURCE_LANG` | Default source language for VideoLingo (optional). |
| `VL_TARGET_LANG` | Default target language (optional). |
| `VL_ENABLE_DUBBING` | Set to `1` to toggle dubbing pipeline. |
| `VL_HLS_SEGMENT` | HLS segment duration in seconds (default `6`). |

AWS credentials for talking to S3 are the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, etc., and are read by `boto3`.

## Collections and Documents

- **posts** (fields referenced by the worker):
  - `video.hls_s3_key` (or `video.hls_input_key`): key under `cw-dev-hls-input-bucket` that points to the master playlist.
  - `has_subtitles`: boolean set when subtitles are available.
  - `subtitle_vtt_url`, `subtitle_hls_master`, etc.: filled from the VideoLingo manifest.
- **videolingo_jobs**:
  - `job_id`, `post_id`, `s3_input_key`, `output_s3_prefix`, `status` (pending/running/completed/failed), timestamps, `pod_response`, and the stored `manifest`.

## Flow

1. The change stream watcher sees a post that matches the whitelist, has `video.hls_s3_key`, and `has_subtitles == false`, then inserts a `pending` job.
2. The dispatcher picks a job, POSTs to `VL_POD_RUN_URL` with `job_id`, `s3_input_key`, `s3_output_prefix`, langs, etc. The Pod generates a presigned input URL, runs `cli_run.py`, and uploads outputs to `cw-dev-hls-output-bucket/<prefix>/...`.
3. After the HTTP call returns success, the worker reads `manifest.json` from S3, updates the job to `completed`, and patches the post with the returned URLs (VTT/HLS master/etc.) so the frontend can expose the subtitle toggle.

This is the MVP loop that the prompt describes: **cap-flow orchestrates**, **Pod runs VideoLingo**, **S3 stores artifacts**, and **Mongo exposes links to the app**.
