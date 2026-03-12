# src/core/common.py
from __future__ import annotations

import os
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Optional
from zoneinfo import ZoneInfo

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

WIN_INVALID_CHARS = set('<>:"/\\|?*')


class S3Utils:
    @staticmethod
    def parse_s3_path(s3_path: str) -> Tuple[str, str]:
        """
        Accepts:
          - 's3://bucket'
          - 's3://bucket/prefix'
          - 'bucket'
          - 'bucket/prefix'
          - Access Point / Outposts ARN also allowed in place of "bucket"
        Returns (bucket_or_arn, prefix) with prefix possibly ''.
        """
        if not s3_path or not s3_path.strip():
            raise ValueError("Provide a non-empty S3 path, e.g., s3://my-bucket/folder/")

        s = s3_path.strip().strip('"').strip("'")
        if s.lower().startswith("s3://"):
            s = s[5:]
        s = s.lstrip("/")

        if "/" in s and not s.lower().startswith("arn:"):  # keep ARN whole
            bucket, prefix = s.split("/", 1)
        else:
            bucket, prefix = s, ""

        bucket = bucket.strip()
        prefix = prefix.strip()
        if not bucket:
            raise ValueError("Bucket name could not be parsed from the provided path.")
        return bucket, prefix

    @staticmethod
    def to_utc(dt):
        """Make a timezone-aware UTC datetime (S3 LastModified is UTC)."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return dt.astimezone(timezone.utc)

    @staticmethod
    def build_s3_uri(bucket: str, key: str, version_id: str | None = None) -> str:
        if version_id:
            return f"s3://{bucket}/{key}?versionId={version_id}"
        return f"s3://{bucket}/{key}"

    @staticmethod
    def summarize_subfolders(all_keys: List[str], base_prefix: str) -> List[str]:
        """
        Returns a sorted list of top-level subfolder names under base_prefix.
        """
        norm = base_prefix
        if norm and not norm.endswith("/"):
            norm = norm + "/"
        subfolders = set()
        for k in all_keys:
            rel = k[len(norm):] if norm and k.startswith(norm) else (k if not norm else None)
            if rel and "/" in rel:
                subfolders.add(rel.split("/", 1)[0])
        return sorted(subfolders)


class PathUtils:
    @staticmethod
    def sanitize_component(name: str) -> str:
        """Replace Windows-invalid characters with underscores."""
        return "".join("_" if c in WIN_INVALID_CHARS else c for c in name)

    @staticmethod
    def build_local_path(dest_dir: Path, key: str, base_prefix: str, preserve_structure: bool) -> Path:
        """
        If preserve_structure=True, create a path relative to base_prefix under dest_dir.
        Otherwise, use the basename of the key in dest_dir. Windows-safe components.
        """
        if not preserve_structure:
            return dest_dir / PathUtils.sanitize_component(Path(key).name)

        rel = key
        if base_prefix:
            norm = base_prefix if base_prefix.endswith("/") else base_prefix + "/"
            if key.startswith(norm):
                rel = key[len(norm):]
        parts = [PathUtils.sanitize_component(p) for p in Path(rel).parts]
        return dest_dir.joinpath(*parts)

    @staticmethod
    def windows_extended_path(p: Path) -> str:
        """Add Windows extended-length prefix if on Windows (to bypass MAX_PATH=260)."""
        abs_path = str(p.resolve())
        if os.name == "nt" and not abs_path.startswith("\\\\?\\"):
            return "\\\\?\\" + abs_path
        return abs_path


# ---------------------------------------------------------------------------
# Date / time helpers
# ---------------------------------------------------------------------------

def get_default_date_range() -> Tuple[datetime, datetime]:
    """
    Return a ``(start, end)`` tuple covering the last 24 hours in Sydney time
    (Australia/Sydney, AEDT/AEST).

    Both datetimes are timezone-aware and have microseconds stripped so they
    render cleanly in ``st.datetime_input`` widgets.

    Moved here from ``pages/2_Analyse_S3.py`` where it was defined inline and
    duplicated across multiple call-sites.
    """
    end = datetime.now(tz=SYDNEY_TZ).replace(microsecond=0)
    start = end - timedelta(days=1)
    return start, end


# ---------------------------------------------------------------------------
# S3 key / file-type helpers
# ---------------------------------------------------------------------------

def extract_file_extension(key: str) -> str:
    """
    Extract a normalised file extension from an S3 object key.

    Handles compound extensions such as ``.csv.gz``, ``.json.gz``, ``.xml.gz``
    before falling back to the last dotted segment.

    Examples::

        extract_file_extension("data/file.csv.gz")  # → "csv.gz"
        extract_file_extension("data/file.parquet") # → "parquet"
        extract_file_extension("data/no_ext")       # → ""

    Moved here from ``pages/2_Analyse_S3.py`` where it was defined as the
    private helper ``_coalesce_ext()``.
    """
    k = (key or "").lower()
    if k.endswith(".json.gz") or k.endswith(".ndjson.gz") or k.endswith(".jsonl.gz"):
        return "jsonl.gz"
    if k.endswith(".csv.gz"):
        return "csv.gz"
    if k.endswith(".xml.gz"):
        return "xml.gz"
    m = re.search(r"\.([a-z0-9]+)$", k)
    return m.group(1) if m else ""