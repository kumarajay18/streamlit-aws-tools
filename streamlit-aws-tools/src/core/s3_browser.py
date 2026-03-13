# src/core/s3_browser.py
from __future__ import annotations

import re
from typing import List, Dict, Optional
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from .common import S3Utils


class S3Browser:
    """Reusable S3 listing and discovery utilities."""

    def __init__(self, boto3_client):
        self.s3 = boto3_client

    # -------------------------
    # Internal helpers
    # -------------------------
    @staticmethod
    def _to_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
        """
        Normalize any datetime to timezone-aware UTC.
        If dt is naive, assume it's UTC (S3 returns tz-aware UTC, but be defensive).
        """
        if dt is None:
            return None
        if dt.tzinfo is None:
            # If your environment expects naive dt to be *local*, change to:
            # dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _in_window(lm_utc: Optional[datetime], start_utc: Optional[datetime], end_utc: Optional[datetime]) -> bool:
        """
        Inclusive time window check in UTC.
        """
        if lm_utc is None:
            # If there's no LastModified, exclude by default; change to True if you want to include unknowns
            return False
        if start_utc and lm_utc < start_utc:
            return False
        if end_utc and lm_utc > end_utc:
            return False
        return True

    @staticmethod
    def _literal_prefix(pattern: str) -> str:
        """Return the longest leading literal (non-regex) portion of *pattern*.

        This is the narrowest prefix we can pass to the S3 API so the
        server-side scan is efficient.  Client-side regex filtering is then
        applied to the full *pattern* after the listing.

        Examples::

            "entity/flight"          -> "entity/flight"   # no metacharacters
            "entity/flight.*"        -> "entity/flight"
            "entity/(flight|train)"  -> "entity/"
            ""                       -> ""
        """
        special = set(r'\.^$*+?{}[]|()')
        for i, ch in enumerate(pattern):
            if ch in special:
                return pattern[:i]
        return pattern

    @staticmethod
    def _compile_prefix_pattern(pattern: str) -> Optional[re.Pattern]:
        """Compile *pattern* as a regex for client-side key filtering.

        Returns ``None`` when the pattern is empty so callers can skip the
        filter cheaply.  Falls back to ``None`` on invalid regex (the literal
        S3 prefix scan is still used in that case).
        """
        if not pattern:
            return None
        try:
            return re.compile(pattern)
        except re.error:
            return None

    # ---------- Current objects ----------

    def list_objects(self, bucket: str, prefix: str, cap: int,
                     start_utc: Optional[datetime] = None, end_utc: Optional[datetime] = None) -> List[Dict]:
        """
        Returns rows: {Key, Size (MB), LastModified, StorageClass, S3 URI}
        LastModified is timezone-aware UTC.

        *prefix* may contain regex metacharacters.  The longest literal leading
        portion is used as the S3 API prefix so the server scan is narrow;
        the full pattern is then applied client-side with ``re.search``.

        When a time filter (start_utc / end_utc) is active the AWS
        ``MaxItems`` pagination cap is **removed** so that every object in the
        prefix is inspected before the time window is applied.  Without this,
        objects that match the time range but sit beyond position *cap* in S3's
        alphabetical ordering would never be seen.  Results are still capped at
        *cap* after filtering.
        """
        # Normalize bounds to UTC-aware
        start_utc = self._to_utc_aware(start_utc) if start_utc else None
        end_utc = self._to_utc_aware(end_utc) if end_utc else None

        # Separate literal S3 prefix from the optional regex suffix
        literal_pfx = self._literal_prefix(prefix or "")
        key_pattern = self._compile_prefix_pattern(prefix or "")

        paginator = self.s3.get_paginator("list_objects_v2")

        # When a time filter is active we must scan ALL pages first — MaxItems
        # would cut the AWS scan before we reach objects in the target window.
        time_filtering = bool(start_utc or end_utc)
        if time_filtering:
            pages = paginator.paginate(
                Bucket=bucket,
                Prefix=literal_pfx,
                PaginationConfig={"PageSize": 1000},
            )
        else:
            pages = paginator.paginate(
                Bucket=bucket,
                Prefix=literal_pfx,
                PaginationConfig={"MaxItems": cap, "PageSize": min(cap, 1000)},
            )

        rows: List[Dict] = []
        count = 0
        for page in pages:
            contents = page.get("Contents", []) or []
            for obj in contents:
                if count >= cap:
                    break

                key = obj.get("Key")
                lm_raw = obj.get("LastModified")
                lm_utc = self._to_utc_aware(lm_raw)

                if not self._in_window(lm_utc, start_utc, end_utc):
                    continue

                # Apply client-side regex filter when the user supplied a pattern
                if key_pattern and not key_pattern.search(key or ""):
                    continue

                rows.append({
                    "Key": key,
                    "Size (MB)": round((obj.get("Size", 0) or 0) / (1024 * 1024), 3),
                    "LastModified": lm_utc,
                    "StorageClass": obj.get("StorageClass", "STANDARD"),
                    "S3 URI": S3Utils.build_s3_uri(bucket, key),
                })
                count += 1
            if count >= cap:
                break
        return rows

    def find_latest_object(self, bucket: str, prefix: str,
                           start_utc: Optional[datetime] = None, end_utc: Optional[datetime] = None) -> Optional[Dict]:
        """
        Finds the latest *current* object (ignores versions/delete markers).
        Returns dict with Key, Size (MB), LastModified (UTC), StorageClass, S3 URI.
        """
        start_utc = self._to_utc_aware(start_utc) if start_utc else None
        end_utc = self._to_utc_aware(end_utc) if end_utc else None

        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix or "")

        latest_obj = None
        latest_lm_utc = None
        for page in pages:
            contents = page.get("Contents", []) or []
            for obj in contents:
                lm_utc = self._to_utc_aware(obj.get("LastModified"))
                if not self._in_window(lm_utc, start_utc, end_utc):
                    continue
                if (latest_lm_utc is None) or (lm_utc and lm_utc > latest_lm_utc):
                    latest_lm_utc = lm_utc
                    latest_obj = obj

        if not latest_obj:
            return None

        key = latest_obj.get("Key")
        return {
            "Key": key,
            "Size (MB)": round((latest_obj.get("Size", 0) or 0) / (1024 * 1024), 3),
            "LastModified": latest_lm_utc,
            "StorageClass": latest_obj.get("StorageClass", "STANDARD"),
            "S3 URI": S3Utils.build_s3_uri(bucket, key),
        }

    # ---------- Versions (including delete markers) ----------

    def list_object_versions(self, bucket: str, prefix: str, cap: int,
                             start_utc: Optional[datetime] = None, end_utc: Optional[datetime] = None,
                             include_delete_markers: bool = True) -> List[Dict]:
        """
        Returns rows: {Key, VersionId, IsLatest, IsDeleteMarker, Size (MB), LastModified, StorageClass, S3 URI}
        LastModified is timezone-aware UTC.

        Same *prefix*-as-regex and time-filter scanning rules as
        :meth:`list_objects` apply here.
        """
        start_utc = self._to_utc_aware(start_utc) if start_utc else None
        end_utc = self._to_utc_aware(end_utc) if end_utc else None

        literal_pfx = self._literal_prefix(prefix or "")
        key_pattern = self._compile_prefix_pattern(prefix or "")

        paginator = self.s3.get_paginator("list_object_versions")

        time_filtering = bool(start_utc or end_utc)
        if time_filtering:
            pages = paginator.paginate(
                Bucket=bucket,
                Prefix=literal_pfx,
                PaginationConfig={"PageSize": 1000},
            )
        else:
            pages = paginator.paginate(
                Bucket=bucket,
                Prefix=literal_pfx,
                PaginationConfig={"MaxItems": cap, "PageSize": min(cap, 1000)},
            )

        rows: List[Dict] = []
        count = 0

        for page in pages:
            # Versions
            for v in page.get("Versions", []) or []:
                if count >= cap:
                    break
                lm_utc = self._to_utc_aware(v.get("LastModified"))
                if not self._in_window(lm_utc, start_utc, end_utc):
                    continue
                key = v.get("Key")
                if key_pattern and not key_pattern.search(key or ""):
                    continue
                rows.append({
                    "Key": key,
                    "VersionId": v.get("VersionId"),
                    "IsLatest": v.get("IsLatest", False),
                    "IsDeleteMarker": False,
                    "Size (MB)": round((v.get("Size", 0) or 0) / (1024 * 1024), 3),
                    "LastModified": lm_utc,
                    "StorageClass": v.get("StorageClass", "STANDARD"),
                    "S3 URI": S3Utils.build_s3_uri(bucket, v.get("Key"), v.get("VersionId")),
                })
                count += 1
            if count >= cap:
                break

            # Delete markers
            if include_delete_markers and count < cap:
                for dm in page.get("DeleteMarkers", []) or []:
                    if count >= cap:
                        break
                    lm_utc = self._to_utc_aware(dm.get("LastModified"))
                    if not self._in_window(lm_utc, start_utc, end_utc):
                        continue
                    key = dm.get("Key")
                    if key_pattern and not key_pattern.search(key or ""):
                        continue
                    rows.append({
                        "Key": key,
                        "VersionId": dm.get("VersionId"),
                        "IsLatest": dm.get("IsLatest", False),
                        "IsDeleteMarker": True,
                        "Size (MB)": None,
                        "LastModified": lm_utc,
                        "StorageClass": None,
                        "S3 URI": S3Utils.build_s3_uri(bucket, dm.get("Key"), dm.get("VersionId")),
                    })
                    count += 1
            if count >= cap:
                break

        return rows

    def summarize_subfolders(self, keys: List[str], base_prefix: str) -> List[str]:
        return S3Utils.summarize_subfolders(keys, base_prefix)