# src/core/s3_uploader.py
from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Iterable, Tuple, Optional

from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig


class S3Uploader:
    def __init__(self, boto3_client):
        self.s3 = boto3_client

    @staticmethod
    def iter_local_files(root: Path) -> Iterable[Path]:
        if root.is_file():
            yield root
        elif root.is_dir():
            for p in root.rglob("*"):
                if p.is_file():
                    yield p
        else:
            raise FileNotFoundError(f"Local path not found: {root}")

    @staticmethod
    def relative_key(local_root: Path, file_path: Path, dest_prefix: str, preserve_structure: bool) -> str:
        prefix = dest_prefix.strip()
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        if preserve_structure:
            rel = file_path.relative_to(local_root)
            rel_str = str(rel).replace("\\", "/")
            return f"{prefix}{rel_str}" if prefix else rel_str
        else:
            name = file_path.name
            return f"{prefix}{name}" if prefix else name

    @staticmethod
    def guess_content_type(file_path: Path) -> Optional[str]:
        ctype, _ = mimetypes.guess_type(str(file_path))
        return ctype

    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def upload_one(self,
                   bucket: str,
                   key: str,
                   file_path: Path,
                   content_type: Optional[str],
                   overwrite: bool,
                   transfer_cfg: TransferConfig,
                   sse: Optional[str] = None,
                   kms_key_id: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if sse == "AES256":
            extra_args["ServerSideEncryption"] = "AES256"
        elif sse == "aws:kms":
            extra_args["ServerSideEncryption"] = "aws:kms"
            if kms_key_id:
                extra_args["SSEKMSKeyId"] = kms_key_id

        if not overwrite and self.object_exists(bucket, key):
            return False, "Skipped (exists and overwrite disabled)"
        try:
            self.s3.upload_file(
                Filename=str(file_path),
                Bucket=bucket,
                Key=key,
                ExtraArgs=extra_args if extra_args else None,
                Config=transfer_cfg,
            )
            return True, None
        except Exception as e:
            return False, str(e)

    @staticmethod
    def fmt_size(nbytes: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(nbytes)
        for i, u in enumerate(units):
            if size < 1024.0 or u == "TB":
                return f"{int(size)} B" if u == "B" else f"{size:.1f} {u}"
            size /= 1024.0
        return f"{size:.1f} TB"