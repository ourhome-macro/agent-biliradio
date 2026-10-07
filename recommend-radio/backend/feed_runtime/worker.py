from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

from database import get_connection
from job_errors import JobPermanentFailure
from models import Track

from agent_memory_runtime.telemetry import span

from .media import FILE_NAME, MediaService
from .metrics import ACTIVE, BYTES, DURATION, count
from .repository import encode
from .source import BiliMediaSource, MediaUnavailable
from .storage import file_hash


class MediaCancelled(Exception):
    pass


class MediaWorker:
    def __init__(
        self,
        repo,
        config,
        storage,
        *,
        source_factory=BiliMediaSource,
        process_factory=subprocess.Popen,
    ):
        self.repo, self.config, self.storage = repo, config, storage
        self.source_factory, self.process_factory = source_factory, process_factory
        self.media = MediaService(repo, config, storage, source_factory=source_factory)

    def run(self, import_id, *, transport_job=None):
        from .maintenance import cleanup

        cleanup(self.repo, self.config, None, apply=True)
        job = self.repo.claim_import(import_id, self.config)
        if job is None:
            current = self.repo.import_job(import_id)
            if current and current["status"] in {"queued", "running"}:
                raise RuntimeError("MediaImportStillOwned")
            return {
                "status": "ready"
                if current and current["status"] == "completed"
                else current["status"]
                if current
                else "not_found"
            }
        if transport_job:
            job["transport_token"] = transport_job["lease_token"]
        ACTIVE.inc()
        import_started = time.monotonic()
        job["deadline"] = import_started + self.config.import_budget_seconds
        stop = threading.Event()

        def renew():
            while not stop.wait(self.config.heartbeat_seconds):
                try:
                    self.repo.progress(job, self.config)
                except Exception:
                    return

        heartbeat = threading.Thread(target=renew, name="media-lease", daemon=True)
        heartbeat.start()
        try:
            with span(
                "media.import",
                attributes={"media.asset_id": job["asset_id"], "media.import_id": import_id},
            ) as trace:
                if job["content_status"] != "admitted":
                    raise JobPermanentFailure("Content is retired")
                if not job["complete_source"]:
                    self._acquire(job)
                directory = self.media.workdir(job)
                manifest = self._package(job, directory)
                self.repo.reserve_storage(job, manifest["bytes"], self.config)
                self.repo.progress(
                    job,
                    self.config,
                    stage="uploading",
                    source_complete=True,
                    byte_count=manifest["bytes"],
                )
                self.storage.ensure_bucket()
                uploaded = {}
                # Manifest publishes last. An incomplete prefix is never a ready asset.
                for name in sorted(manifest["names"], key=lambda n: n == "index.m3u8"):
                    self.repo.progress(job, self.config, stage="uploading")
                    mime = (
                        "application/vnd.apple.mpegurl"
                        if name.endswith("m3u8")
                        else "image/jpeg"
                        if name.endswith("jpg")
                        else "application/json"
                        if name.endswith("json")
                        else "audio/mp4"
                        if name.endswith("m4a")
                        else "video/mp4"
                    )
                    key = f"assets/{job['asset_id']}/{manifest['digest']}/{name}"
                    with DURATION.labels("upload").time():
                        uploaded[name] = self.storage.put_file(
                            key, directory / name, content_type=mime
                        )
                    BYTES.labels("object_upload").inc(uploaded[name]["bytes"])
                persisted = {
                    "files": uploaded,
                    "playlist": manifest["playlist"],
                    "duration": manifest["duration"],
                    "bytes": manifest["bytes"],
                    "format": "hls-fmp4-v1",
                }
                self.repo.complete_import(job, persisted)
                trace.set_attribute("media.outcome", "ready")
                count("import", "ready")
                return {"status": "ready", "assetId": job["asset_id"]}
        except MediaCancelled:
            if self.repo.import_job(import_id)["status"] != "cancelled":
                self.repo.stop_import(job, status="cancelled")
            self._remove_workdir(job)
            count("import", "cancelled")
            return {"status": "cancelled"}
        except Exception as error:
            complete = self.repo.import_job(import_id)["complete_source"]
            retry = bool(
                complete and transport_job and transport_job["attempts"] < self.config.max_attempts
            )
            if str(error) not in {"MediaLeaseLost", "TransportLeaseLost"}:
                self.repo.stop_import(
                    job, status="queued" if retry else "failed", error_type=type(error).__name__
                )
            unavailable = isinstance(error, MediaUnavailable)
            if unavailable or (
                isinstance(error, ValueError)
                and str(error)
                in {"UnsupportedMediaCodec", "MediaDurationExceeded", "MediaAssetQuotaExceeded"}
            ):
                with get_connection(self.repo.db_path) as conn:
                    conn.execute(
                        "UPDATE media_assets SET status=? WHERE asset_id=? AND status<>'ready'",
                        ("retired" if unavailable else "unsupported", job["asset_id"]),
                    )
            count("import", "retry" if retry else "failed")
            if retry:
                raise RuntimeError("MediaObjectUploadRetry") from None
            raise JobPermanentFailure(f"Media import failed: {type(error).__name__}") from None
        finally:
            stop.set()
            heartbeat.join(timeout=2)
            ACTIVE.dec()
            DURATION.labels("import").observe(time.monotonic() - import_started)

    def _acquire(self, job):
        prior_generations = job["fetch_attempts"]
        for generation in range(1, self.config.max_attempts + 1):
            if not self.repo.demand(job, self.config) and self.repo.cancel_if_idle(
                job, self.config
            ):
                raise MediaCancelled()
            sequence = prior_generations + generation
            work_key = f"{job['asset_id']}/{job['import_id']}/{job['lease_token']}/fetch-{sequence}"
            with get_connection(self.repo.db_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                self.repo.owned(conn, job)
                conn.execute(
                    "UPDATE media_import_jobs SET work_key=?,fetch_attempts=? WHERE import_id=?",
                    (work_key, sequence, job["import_id"]),
                )
            job.update(work_key=work_key, fetch_attempts=sequence)
            directory = self.media.workdir(job)
            directory.mkdir(parents=True, exist_ok=True)
            source = self.source_factory(
                user_id=job["owner_id"] or "legacy-owner",
                scope=job["scope"],
                db_path=self.repo.db_path,
            )
            try:
                resolved = source.resolve(Track.from_dict(json.loads(job["metadata_json"])))
                if resolved.duration <= 0 or resolved.duration > self.config.max_duration:
                    raise ValueError("MediaDurationExceeded")
                (directory / "source.json").write_text(
                    encode({"duration": resolved.duration}), encoding="utf-8"
                )
                self.repo.progress(job, self.config, stage="fetching")
                headers = "".join(
                    f"{k}: {v}\r\n"
                    for k, v in resolved.headers.items()
                    if k.lower() in {"referer", "user-agent"}
                )
                command = [
                    self.config.ffmpeg,
                    "-nostdin",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                ]
                for url in (resolved.video_url, resolved.audio_url):
                    if url.startswith(("http://", "https://")):
                        command.extend(["-rw_timeout", "15000000", "-headers", headers])
                    command.extend(["-i", url])
                command.extend(
                    [
                        "-map",
                        "0:v:0",
                        "-map",
                        "1:a:0",
                        "-c",
                        "copy",
                        "-f",
                        "hls",
                        "-hls_time",
                        "4",
                        "-hls_playlist_type",
                        "event",
                        "-hls_segment_type",
                        "fmp4",
                        "-hls_flags",
                        "temp_file+independent_segments",
                        "-hls_segment_filename",
                        "seg_%06d.m4s",
                        "index.m3u8",
                    ]
                )
                self._fetch_process(job, directory, command)
                self._verify(directory, resolved.duration, timeout=self._remaining(job, 60))
                self.repo.progress(job, self.config, stage="verifying", source_complete=True)
                job["complete_source"] = 1
                return
            except MediaCancelled:
                raise
            except Exception as error:
                if (
                    generation == self.config.max_attempts
                    or isinstance(error, ValueError)
                    or isinstance(error, TimeoutError)
                    or str(error) in {"MediaLeaseLost", "TransportLeaseLost"}
                ):
                    raise
                self._remove_workdir(job)
                time.sleep(min(2**generation, 8))
            finally:
                source.close()

    def _fetch_process(self, job, directory, command):
        kwargs = {"cwd": str(directory), "stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        with (directory / "process.stderr").open("wb") as stderr:
            process = self.process_factory(command, stderr=stderr, **kwargs)
            try:
                last_renew = 0.0
                first_fragment = False
                while process.poll() is None:
                    if not first_fragment and (directory / "index.m3u8").exists():
                        playlist = (directory / "index.m3u8").read_text(encoding="utf-8")
                        first_fragment = any(
                            FILE_NAME.fullmatch(line)
                            and line.startswith("seg_")
                            and (directory / line).is_file()
                            for line in playlist.splitlines()
                        )
                        if first_fragment:
                            DURATION.labels("cold_first_fragment").observe(
                                time.time() - job["created_at"]
                            )
                    if not self.repo.demand(job, self.config) and self.repo.cancel_if_idle(
                        job, self.config
                    ):
                        raise MediaCancelled()
                    if time.monotonic() - last_renew >= self.config.heartbeat_seconds:
                        size = sum(p.stat().st_size for p in directory.iterdir() if p.is_file())
                        scratch = sum(
                            p.stat().st_size for p in self.config.workdir.rglob("*") if p.is_file()
                        )
                        if size > self.config.max_bytes or scratch > self.config.scratch_bytes:
                            raise ValueError("MediaScratchQuotaExceeded")
                        self.repo.progress(job, self.config, stage="fetching", byte_count=size)
                        last_renew = time.monotonic()
                    time.sleep(0.25)
                if process.returncode != 0:
                    raise RuntimeError("MediaFetchProcessFailed")
                # Natural EOF wins over a simultaneous viewer detach.
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)

    @staticmethod
    def _remaining(job, maximum):
        remaining = job["deadline"] - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("MediaExecutionBudgetExceeded")
        return min(maximum, remaining)

    def _verify(self, directory, expected_duration, *, timeout=60):
        playlist = (directory / "index.m3u8").read_text(encoding="utf-8")
        if "#EXT-X-ENDLIST" not in playlist or '#EXT-X-MAP:URI="init.mp4"' not in playlist:
            raise RuntimeError("IncompleteMediaPlaylist")
        for line in playlist.splitlines():
            if line and not line.startswith("#"):
                if not FILE_NAME.fullmatch(line) or not (directory / line).is_file():
                    raise RuntimeError("IncompleteMediaSegment")
        result = subprocess.run(
            [
                self.config.ffprobe,
                "-v",
                "error",
                "-show_entries",
                "format=duration:stream=codec_name,codec_type",
                "-of",
                "json",
                "index.m3u8",
            ],
            cwd=directory,
            capture_output=True,
            timeout=timeout,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        if result.returncode:
            raise RuntimeError("MediaProbeFailed")
        parsed = json.loads(result.stdout)
        codecs = {(s["codec_type"], s["codec_name"]) for s in parsed["streams"]}
        duration = float(parsed["format"]["duration"])
        if not {("video", "h264"), ("audio", "aac")} <= codecs or abs(
            duration - expected_duration
        ) > max(3, expected_duration * 0.02):
            raise RuntimeError("MediaTrackOrDurationMismatch")
        return duration

    def _package(self, job, directory):
        expected = json.loads((directory / "source.json").read_text(encoding="utf-8"))["duration"]
        duration = self._verify(directory, expected, timeout=self._remaining(job, 60))
        for filename, options in (
            ("audio.m4a", ["-map", "0:a:0", "-c:a", "copy", "-movflags", "+faststart"]),
            ("cover.jpg", ["-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "4"]),
        ):
            self.repo.progress(job, self.config, stage="packaging", source_complete=True)
            if not (directory / filename).is_file():
                target = Path(filename)
                temporary = f"{target.stem}.{job['lease_token']}.tmp{target.suffix}"
                result = subprocess.run(
                    [
                        self.config.ffmpeg,
                        "-nostdin",
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-y",
                        "-i",
                        "index.m3u8",
                        *options,
                        temporary,
                    ],
                    cwd=directory,
                    capture_output=True,
                    timeout=self._remaining(job, 120),
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
                if result.returncode:
                    raise RuntimeError("MediaPackagingFailed")
                with get_connection(self.repo.db_path) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    self.repo.owned(conn, job)
                    (directory / temporary).replace(directory / filename)
        (directory / "metadata.json").write_text(
            encode(
                {
                    "assetId": job["asset_id"],
                    "duration": duration,
                    "videoCodec": "h264",
                    "audioCodec": "aac",
                    "source": "bili",
                    "scope": job["scope"],
                }
            ),
            encoding="utf-8",
        )
        names = sorted(
            p.name
            for p in directory.iterdir()
            if p.is_file() and (FILE_NAME.fullmatch(p.name) or p.name == "metadata.json")
        )
        size = sum((directory / name).stat().st_size for name in names)
        if size > self.config.max_bytes:
            raise ValueError("MediaAssetQuotaExceeded")
        return {
            "names": names,
            "digest": hashlib.sha256(
                encode({name: file_hash(directory / name) for name in names}).encode()
            ).hexdigest(),
            "bytes": size,
            "duration": duration,
            "playlist": (directory / "index.m3u8").read_text(encoding="utf-8"),
        }

    def _remove_workdir(self, job):
        directory = self.media.workdir(job)
        # resolve() above fences the recursive removal to our dedicated workspace.
        if directory.exists() and directory.is_relative_to(self.config.workdir.resolve()):
            shutil.rmtree(directory)
