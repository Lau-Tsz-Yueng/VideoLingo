# Summary
- Added `.env` loading for CLI and server so AWS/S3 and language defaults can be managed from the repo; included sample AWS credentials in `.env` (replace with your own/rotate in real use).
- Built an S3-driven poller (`s3_poller.py`) that discovers new `.m3u8` playlists, downloads the full HLS prefix locally, runs `cli_run.py`, uploads outputs, and records markers; supports failed-job retries with a configurable max retry count and staging directory.
- Hardened HLS URL handling in `cli_run.py` to detect `.m3u8` even with query params and to auto-ensure CJK font availability before burning subtitles.

# Testing
- `python -m py_compile cli_run.py`
- `python -m py_compile server.py`
- `python -m py_compile s3_poller.py`
