from __future__ import annotations

import hashlib
from datetime import timedelta
from pathlib import Path

from .config import FeedConfig


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class MinIOStorage:
    def __init__(self, config: FeedConfig, *, client=None, public_client=None):
        from minio import Minio
        from urllib3 import PoolManager
        from urllib3.util import Timeout

        self.config = config
        self.client = client or Minio(
            config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
            region="us-east-1",
            http_client=PoolManager(maxsize=8, timeout=Timeout(connect=3, read=30), retries=0),
        )
        self.public = public_client or Minio(
            config.minio_public_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_public_secure,
            region="us-east-1",
        )

    def health(self) -> bool:
        try:
            return bool(self.client.bucket_exists(self.config.bucket))
        except Exception:
            return False

    def ensure_bucket(self):
        if not self.client.bucket_exists(self.config.bucket):
            from minio.error import S3Error

            try:
                self.client.make_bucket(self.config.bucket)
            except S3Error as error:
                if error.code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise

    def prepare_restore_bucket(self):
        """Restore into a newly created namespace, never an active business bucket."""
        from minio.error import S3Error
        try:
            self.client.make_bucket(self.config.bucket)
        except S3Error as error:
            if error.code in {"BucketAlreadyOwnedByYou","BucketAlreadyExists"}:
                raise ValueError("Restore requires a new dedicated MinIO bucket") from None
            raise

    def stat(self, key):
        from minio.error import S3Error

        try:
            value = self.client.stat_object(self.config.bucket, key)
            return {
                "bytes": value.size,
                "sha256": value.metadata.get("x-amz-meta-sha256", ""),
                "versionId": value.version_id,
            }
        except S3Error as error:
            if error.code in {"NoSuchKey", "NoSuchObject"}:
                return None
            raise

    def put_file(self, key: str, path: Path, *, content_type: str):
        size, digest = path.stat().st_size, file_hash(path)
        old = self.stat(key)
        if old and old["bytes"] == size and old["sha256"] == digest:
            return {"key": key, **old}
        result = self.client.fput_object(
            self.config.bucket,
            key,
            str(path),
            content_type=content_type,
            metadata={"sha256": digest},
        )
        verified = self.stat(key)
        if not verified or verified["bytes"] != size or verified["sha256"] != digest:
            raise RuntimeError("ObjectVerificationFailed")
        return {"key": key, "bytes": size, "sha256": digest, "versionId": result.version_id}

    def presign(self, key: str, *, version_id=None) -> str:
        return self.public.presigned_get_object(
            self.config.bucket,
            key,
            expires=timedelta(seconds=self.config.signed_seconds),
            version_id=version_id,
        )

    def remove(self, key: str, *, version_id=None):
        self.client.remove_object(self.config.bucket, key, version_id=version_id)
