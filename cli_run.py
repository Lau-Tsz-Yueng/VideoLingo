import argparse
import json
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from batch.utils.video_processor import process_video
from core.utils.config_utils import load_key, update_key
from core.utils.onekeycleanup import sanitize_filename


INPUT_DIR = Path("batch/input")
OUTPUT_HISTORY_DIR = Path("batch/output")
HLS_DEFAULT_SEGMENT = 6


def _make_job_id(video_path: str, provided: Optional[str]) -> str:
    if provided:
        return sanitize_filename(provided)
    stem = Path(video_path).stem or "job"
    stem = sanitize_filename(stem)
    return f"{stem}-{uuid.uuid4().hex[:8]}"


def _ensure_dirs():
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _run_ffmpeg(args):
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        stderr = proc.stderr.decode(errors="ignore")
        raise RuntimeError(f"ffmpeg failed: {stderr.strip()}")


def _materialize_local_copy(src: Path, dest: Path, convert_ts: bool = False):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if convert_ts:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(src),
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            str(dest),
        ]
        _run_ffmpeg(cmd)
    else:
        shutil.copy2(src, dest)


def _prepare_input(video_path: str, job_id: str) -> tuple[str, Optional[str]]:
    path = Path(video_path)
    if video_path.startswith("http"):
        if video_path.lower().endswith(".m3u8"):
            dest = INPUT_DIR / f"{job_id}.mp4"
            cmd = [
                "ffmpeg",
                "-y",
                "-protocol_whitelist",
                "file,http,https,tcp,tls",
                "-i",
                video_path,
                "-c",
                "copy",
                str(dest),
            ]
            _run_ffmpeg(cmd)
            return dest.name, sanitize_filename(dest.stem)
        return video_path, None

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {video_path}")

    suffix = path.suffix.lower()
    is_ts = suffix == ".ts"
    is_m3u8 = suffix == ".m3u8"
    dest_suffix = ".mp4" if is_ts or is_m3u8 else suffix or ".mp4"
    dest = INPUT_DIR / f"{job_id}{dest_suffix}"

    if is_m3u8:
        cmd = [
            "ffmpeg",
            "-y",
            "-protocol_whitelist",
            "file,http,https,tcp,tls",
            "-i",
            str(path),
            "-c",
            "copy",
            str(dest),
        ]
        _run_ffmpeg(cmd)
    else:
        _materialize_local_copy(path, dest, convert_ts=is_ts)

    return dest.name, sanitize_filename(dest.stem)


def _srt_to_vtt(srt_path: Path, vtt_path: Path):
    cmd = ["ffmpeg", "-y", "-i", str(srt_path), str(vtt_path)]
    _run_ffmpeg(cmd)


def _write_master_playlist(master_path: Path, stream_name: str, has_subs: bool):
    video_playlist = "video.m3u8"
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
    ]
    if has_subs:
        lines.append(
            '#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Translated",DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="zh",URI="subtitles.vtt"'
        )
        lines.append(
            f'#EXT-X-STREAM-INF:BANDWIDTH=800000,NAME="{stream_name}",SUBTITLES="subs"'
        )
    else:
        lines.append(f'#EXT-X-STREAM-INF:BANDWIDTH=800000,NAME="{stream_name}"')
    lines.append(video_playlist)
    master_path.write_text("\n".join(lines))


def _create_hls_outputs(
    video_file: Path,
    output_dir: Path,
    subtitle_srt: Optional[Path] = None,
    segment_seconds: int = HLS_DEFAULT_SEGMENT,
):
    hls_dir = output_dir / "hls"
    hls_dir.mkdir(parents=True, exist_ok=True)

    video_playlist = hls_dir / "video.m3u8"
    segment_pattern = hls_dir / "segment%03d.ts"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_file),
        "-codec",
        "copy",
        "-hls_time",
        str(segment_seconds),
        "-hls_playlist_type",
        "vod",
        "-start_number",
        "0",
        "-hls_segment_filename",
        str(segment_pattern),
        str(video_playlist),
    ]
    _run_ffmpeg(cmd)

    has_subs = False
    if subtitle_srt and subtitle_srt.exists():
        vtt_path = hls_dir / "subtitles.vtt"
        _srt_to_vtt(subtitle_srt, vtt_path)
        has_subs = True

    master = hls_dir / "master.m3u8"
    _write_master_playlist(master, stream_name=video_file.stem, has_subs=has_subs)
    return hls_dir


def _update_languages(source_lang: Optional[str], target_lang: Optional[str]):
    original_source = load_key("whisper.language")
    original_target = load_key("target_language")

    if source_lang:
        update_key("whisper.language", source_lang)
    if target_lang:
        update_key("target_language", target_lang)

    return original_source, original_target


def _restore_languages(original_source: str, original_target: str):
    update_key("whisper.language", original_source)
    update_key("target_language", original_target)


def _locate_output_dir(expected_name: Optional[str]) -> Optional[Path]:
    if expected_name:
        candidate = OUTPUT_HISTORY_DIR / expected_name
        if candidate.exists():
            return candidate

    if not OUTPUT_HISTORY_DIR.exists():
        return None

    dirs = [p for p in OUTPUT_HISTORY_DIR.iterdir() if p.is_dir()]
    if not dirs:
        return None

    return max(dirs, key=lambda p: p.stat().st_mtime)


def _parse_s3_prefix(prefix: str) -> tuple[str, str]:
    parsed = urlparse(prefix)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("output_s3_prefix must be in format s3://bucket/prefix")
    return parsed.netloc, parsed.path.lstrip("/")


def _iter_output_files(output_dir: Path):
    for path in output_dir.rglob("*"):
        if path.is_file():
            yield path


def _build_manifest(job_id: str, s3_prefix: str, output_dir: Path) -> dict:
    def _maybe_add(name: str, rel_path: str, files: dict):
        candidate = output_dir / rel_path
        if candidate.exists():
            files[name] = f"{s3_prefix.rstrip('/')}/{rel_path}"

    files: dict[str, str] = {}
    _maybe_add("hls_master", "hls/master.m3u8", files)
    _maybe_add("hls_playlist", "hls/video.m3u8", files)
    _maybe_add("vtt", "hls/subtitles.vtt", files)
    _maybe_add("srt", "trans.srt", files)
    _maybe_add("mp4_with_subs", "output_sub.mp4", files)

    return {
        "job_id": job_id,
        "status": "success",
        "output_root": s3_prefix.rstrip("/"),
        "files": files,
    }


def _upload_output_to_s3(output_dir: Path, s3_prefix: str, job_id: str):
    import boto3  # Local import to avoid hard dependency when not used

    bucket, key_prefix = _parse_s3_prefix(s3_prefix)
    s3 = boto3.client("s3")

    for path in _iter_output_files(output_dir):
        rel = path.relative_to(output_dir).as_posix()
        key = f"{key_prefix}/{rel}" if key_prefix else rel
        s3.upload_file(str(path), bucket, key)

    manifest = _build_manifest(job_id, s3_prefix, output_dir)
    manifest_key = f"{key_prefix}/manifest.json" if key_prefix else "manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{manifest_key}", manifest


def main():
    parser = argparse.ArgumentParser(description="Run a single VideoLingo job without Excel.")
    parser.add_argument("--video_path", required=True, help="Local video/m3u8/ts file or URL")
    parser.add_argument("--source_lang", help="Override whisper.language (ISO 639-1)")
    parser.add_argument("--target_lang", help="Override target_language")
    parser.add_argument("--dubbing", action="store_true", help="Enable dubbing pipeline")
    parser.add_argument("--job_id", help="Optional job id (used for temp filenames/output)")
    parser.add_argument("--hls_output", action="store_true", help="Package output_sub.mp4 into HLS with optional subtitles")
    parser.add_argument("--hls_segment", type=int, default=HLS_DEFAULT_SEGMENT, help="HLS segment duration in seconds (default: 6)")
    parser.add_argument("--output_s3_prefix", help="If set, upload output directory to this s3://bucket/prefix and emit manifest.json")
    args = parser.parse_args()

    _ensure_dirs()

    job_id = _make_job_id(args.video_path, args.job_id)
    original_source, original_target = _update_languages(args.source_lang, args.target_lang)

    try:
        input_arg, expected_dir = _prepare_input(args.video_path, job_id)
        status, error_step, error_message = process_video(input_arg, dubbing=args.dubbing, is_retry=False)
    finally:
        _restore_languages(original_source, original_target)

    if not status:
        msg = error_message or "unknown error"
        raise SystemExit(f"VideoLingo failed at step '{error_step}': {msg}")

    output_dir = _locate_output_dir(expected_dir)
    if output_dir:
        if args.hls_output:
            video_candidate = output_dir / "output_sub.mp4"
            subtitle_candidate = output_dir / "trans.srt"
            if not video_candidate.exists():
                raise SystemExit(f"HLS packaging requested but video not found: {video_candidate}")
            hls_dir = _create_hls_outputs(
                video_file=video_candidate,
                output_dir=output_dir,
                subtitle_srt=subtitle_candidate if subtitle_candidate.exists() else None,
                segment_seconds=args.hls_segment,
            )
            print(f"✅ Job completed. Outputs at: {output_dir} (HLS packaged in {hls_dir})")
        else:
            print(f"✅ Job completed. Outputs at: {output_dir}")

        if args.output_s3_prefix:
            manifest_loc, manifest = _upload_output_to_s3(output_dir, args.output_s3_prefix, job_id)
            print(f"✅ Uploaded outputs to {args.output_s3_prefix} (manifest: {manifest_loc})")
            print(json.dumps(manifest, ensure_ascii=False, indent=2))
    else:
        print("✅ Job completed. Outputs moved to batch/output (exact folder not found).")


if __name__ == "__main__":
    main()
