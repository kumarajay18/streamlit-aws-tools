# src/config.py
"""
Central configuration for the DPES AWS Tools application.

All hardcoded values that previously appeared as magic literals scattered
across multiple files (pages, src modules) live here.  Import from this
module instead of duplicating constants.

Usage::

    from src.config import DEFAULT_REGION, SUPPORTED_PROFILES, LIST_CAP_DEFAULT
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# AWS defaults
# ---------------------------------------------------------------------------

#: Default AWS region used when AWS_DEFAULT_REGION env-var is not set.
DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")

#: CloudFormation export name that holds the custom S3 endpoint URL.
DEFAULT_EXPORT_NAME: str = os.getenv("S3_ENDPOINT_EXPORT_NAME", "S3CustomEndpoint")

#: Named AWS CLI profiles supported by this dashboard.
SUPPORTED_PROFILES: list[str] = [
    "a1226-nonprod",
    "a1226-dev",
    "a1226-prod",
]

# ---------------------------------------------------------------------------
# S3 browser / listing limits
# ---------------------------------------------------------------------------

#: Default cap for list_objects results (prevents browser hang on huge prefixes).
LIST_CAP_DEFAULT: int = 1_000

#: Maximum cap the UI allows a user to request.
LIST_CAP_MAX: int = 50_000

# ---------------------------------------------------------------------------
# S3 downloader
# ---------------------------------------------------------------------------

#: Chunk size (bytes) used when streaming a versioned S3 object to disk.
DOWNLOAD_CHUNK_SIZE: int = 8 * 1024 * 1024  # 8 MB

#: Maximum Windows path length before switching to a flattened filename.
WINDOWS_MAX_PATH: int = 240

# ---------------------------------------------------------------------------
# S3 uploader
# ---------------------------------------------------------------------------

#: Additional MIME-type mappings not covered by the stdlib ``mimetypes`` module.
EXTRA_MIME_TYPES: dict[str, str] = {
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".parquet": "application/octet-stream",
    ".sql": "text/x-sql",
    ".gz": "application/gzip",
}

# ---------------------------------------------------------------------------
# ETL QA page
# ---------------------------------------------------------------------------

#: How many S3 objects to inspect at most during a single QA run.
QA_LIST_CAP: int = 5_000

#: How many of the most-recent raw date-partitions to show.
RAW_LAST_N_DATES: int = 3

#: How many of the most-recent curated batch-prefixes to show.
CURATED_LAST_N_BATCHES: int = 5
