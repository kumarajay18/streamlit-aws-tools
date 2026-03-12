# src/core/s3_downloader.py
from __future__ import annotations

import os  # <-- ensure imported
from typing import List, Tuple, Optional, Dict
from pathlib import Path

from botocore.exceptions import ClientError
from .common import PathUtils  # <-- import at module level


class S3Downloader:
    """
    Version-aware downloader.
    - Current objects: uses download_file (fast, multipart-aware).
    - Specific versions: streams via get_object(..., VersionId=...).
    """

    def __init__(self, boto3_client):
        self.s3 = boto3_client

    def _download_current(self, bucket: str, key: str, target: Path) -> Tuple[Optional[Path], Optional[str]]:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.s3.download_file(bucket, key, PathUtils.windows_extended_path(target))
            return target, None
        except Exception as e:
            return None, str(e)

    def _download_version(self, bucket: str, key: str, version_id: str, target: Path) -> Tuple[Optional[Path], Optional[str]]:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=key, VersionId=version_id)
            body = obj["Body"]
            # Stream to disk in 8 MB chunks.
            # boto3 StreamingBody does NOT have iter_chunks(); use read(amt) instead.
            with open(PathUtils.windows_extended_path(target), "wb") as f:
                chunk_size = 8 * 1024 * 1024
                while True:
                    chunk = body.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
            return target, None
        except Exception as e:
            return None, str(e)

    def download_one(
        self,
        bucket: str,
        key: str,
        dest_dir: Path,
        base_prefix: str,
        preserve_structure: bool,
        version_id: Optional[str] = None,
        flatten_if_long: bool = True,
        max_len: int = 240,
    ) -> Tuple[Optional[Path], Optional[str]]:
        """
        Download current object or a specific version to dest_dir.
        """
        target = PathUtils.build_local_path(dest_dir, key, base_prefix, preserve_structure)

        # Try intended target first
        if version_id:
            saved, err = self._download_version(bucket, key, version_id, target)
        else:
            saved, err = self._download_current(bucket, key, target)

        if saved is not None or not (flatten_if_long and os.name == "nt"):
            return saved, err

        # Windows: if path is still too long, flatten to basename as fallback
        try:
            # NOTE: Path.resolve() can raise if parent does not exist; we created it above.
            if len(str(target.resolve())) > max_len:
                flat = dest_dir / PathUtils.sanitize_component(Path(key).name)
                if version_id:
                    return self._download_version(bucket, key, version_id, flat)
                else:
                    return self._download_current(bucket, key, flat)
        except Exception:
            # If resolve fails for any reason, still try flattening
            flat = dest_dir / PathUtils.sanitize_component(Path(key).name)
            if version_id:
                return self._download_version(bucket, key, version_id, flat)
            else:
                return self._download_current(bucket, key, flat)

        return saved, err

    def download_many(
        self,
        bucket: str,
        items: List[Dict],
        dest_dir: Path,
        base_prefix: str,
        preserve_structure: bool,
    ) -> Tuple[List[Path], List[Tuple[str, str]]]:
        """
        items: list of dicts with at least {"Key", "VersionId"?}
        Returns (saved_files, failed_items) where failed_items is list of (display_name, error_msg).
        """
        saved: List[Path] = []
        failed: List[Tuple[str, str]] = []

        dest_dir.mkdir(parents=True, exist_ok=True)

        for it in items:
            key = it["Key"]
            vid = it.get("VersionId") if it.get("VersionId") else None

            p, err = self.download_one(
                bucket=bucket,
                key=key,
                dest_dir=dest_dir,
                base_prefix=base_prefix,
                preserve_structure=preserve_structure,
                version_id=vid,
            )
            if p is not None:
                saved.append(p)
            else:
                label = key if not vid else f"{key} (v={vid})"
                failed.append((label, err or "Unknown error"))

        return saved, failed