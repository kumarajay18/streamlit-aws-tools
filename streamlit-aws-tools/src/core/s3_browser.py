# src/core/s3_browser.py
from __future__ import annotations

from typing import List, Dict, Optional, Tuple
from botocore.exceptions import ClientError

from .common import S3Utils


class S3Browser:
    """Reusable S3 listing and discovery utilities."""

    def __init__(self, boto3_client):
        self.s3 = boto3_client

    # ---------- Current objects ----------

    def list_objects(self, bucket: str, prefix: str, cap: int,
                     start_utc=None, end_utc=None) -> List[Dict]:
        """
        Returns rows: {Key, Size (MB), LastModified, StorageClass, S3 URI}
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix or "")

        rows = []
        count = 0
        for page in pages:
            for obj in page.get("Contents", []):
                if count >= cap:
                    break

                key = obj.get("Key")
                lm = obj.get("LastModified")
                if lm and lm.tzinfo is None:
                    # S3 already UTC, but ensure
                    lm = lm.replace(tzinfo=S3Utils.to_utc(lm).tzinfo)

                # time filter inclusive
                if start_utc and lm and lm < start_utc:
                    continue
                if end_utc and lm and lm > end_utc:
                    continue

                rows.append({
                    "Key": key,
                    "Size (MB)": round((obj.get("Size", 0) or 0) / (1024 * 1024), 3),
                    "LastModified": lm,
                    "StorageClass": obj.get("StorageClass", "STANDARD"),
                    "S3 URI": S3Utils.build_s3_uri(bucket, key),
                })
                count += 1
            if count >= cap:
                break
        return rows

    def find_latest_object(self, bucket: str, prefix: str,
                           start_utc=None, end_utc=None) -> Optional[Dict]:
        """
        Finds the latest *current* object (ignores versions/delete markers).
        Returns dict with Key, Size, LastModified, StorageClass, S3 URI.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix or "")

        latest_obj = None
        latest_lm = None
        for page in pages:
            for obj in page.get("Contents", []):
                lm = obj.get("LastModified")
                if lm and lm.tzinfo is None:
                    lm = lm.replace(tzinfo=S3Utils.to_utc(lm).tzinfo)

                if start_utc and lm and lm < start_utc:
                    continue
                if end_utc and lm and lm > end_utc:
                    continue

                if latest_lm is None or (lm and lm > latest_lm):
                    latest_lm = lm
                    latest_obj = obj

        if not latest_obj:
            return None
        key = latest_obj["Key"]
        return {
            "Key": key,
            "Size (MB)": round((latest_obj.get("Size", 0) or 0) / (1024 * 1024), 3),
            "LastModified": latest_lm,
            "StorageClass": latest_obj.get("StorageClass", "STANDARD"),
            "S3 URI": S3Utils.build_s3_uri(bucket, key),
        }

    # ---------- Versions (including delete markers) ----------

    def list_object_versions(self, bucket: str, prefix: str, cap: int,
                             start_utc=None, end_utc=None,
                             include_delete_markers: bool = True) -> List[Dict]:
        """
        Returns rows: {Key, VersionId, IsLatest, IsDeleteMarker, Size (MB), LastModified, StorageClass, S3 URI}
        """
        paginator = self.s3.get_paginator("list_object_versions")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix or "")

        rows = []
        count = 0

        for page in pages:
            # Versions
            for v in page.get("Versions", []):
                if count >= cap:
                    break
                lm = v.get("LastModified")
                if lm and lm.tzinfo is None:
                    lm = lm.replace(tzinfo=S3Utils.to_utc(lm).tzinfo)

                if start_utc and lm and lm < start_utc:
                    continue
                if end_utc and lm and lm > end_utc:
                    continue

                rows.append({
                    "Key": v.get("Key"),
                    "VersionId": v.get("VersionId"),
                    "IsLatest": v.get("IsLatest", False),
                    "IsDeleteMarker": False,
                    "Size (MB)": round((v.get("Size", 0) or 0) / (1024 * 1024), 3),
                    "LastModified": lm,
                    "StorageClass": v.get("StorageClass", "STANDARD"),
                    "S3 URI": S3Utils.build_s3_uri(bucket, v.get("Key"), v.get("VersionId")),
                })
                count += 1
            if count >= cap:
                break

            # Delete markers (deleted files)
            if include_delete_markers:
                for dm in page.get("DeleteMarkers", []):
                    if count >= cap:
                        break
                    lm = dm.get("LastModified")
                    if lm and lm.tzinfo is None:
                        lm = lm.replace(tzinfo=S3Utils.to_utc(lm).tzinfo)

                    if start_utc and lm and lm < start_utc:
                        continue
                    if end_utc and lm and lm > end_utc:
                        continue

                    rows.append({
                        "Key": dm.get("Key"),
                        "VersionId": dm.get("VersionId"),
                        "IsLatest": dm.get("IsLatest", False),
                        "IsDeleteMarker": True,
                        "Size (MB)": None,
                        "LastModified": lm,
                        "StorageClass": None,
                        "S3 URI": S3Utils.build_s3_uri(bucket, dm.get("Key"), dm.get("VersionId")),
                    })
                    count += 1
            if count >= cap:
                break

        return rows

    def summarize_subfolders(self, keys: List[str], base_prefix: str) -> List[str]:
        return S3Utils.summarize_subfolders(keys, base_prefix)