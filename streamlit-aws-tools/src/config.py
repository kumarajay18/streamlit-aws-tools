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

# ---------------------------------------------------------------------------
# Session-state key registry
#
# Using a central registry of session-state key names keeps pages in sync and
# prevents silent breakage when one page writes under a slightly different name
# than another page reads.
#
# Each constant below is the *string key* used with st.session_state.
# ---------------------------------------------------------------------------

class SK:  # SK = Session Keys
    """String constants for every ``st.session_state`` key used across pages."""

    # ---- AWS session (set by app.py / topbar.py) ----
    AWS_PROFILE        = "aws_profile"
    AWS_REGION         = "aws_region"
    S3_ENDPOINT_URL    = "s3_endpoint_url"
    S3_ENDPOINT_OVERRIDE = "s3_endpoint_override"

    # ---- Active-tab indices (used for tab-stability fix) ----
    # Stored in st.query_params so they survive st.rerun() without resetting
    # the visible tab back to 0.
    TAB_ANALYSE_S3     = "tab_analyse_s3"    # "analyse" | "qa"
    TAB_QA_INNER       = "tab_qa_inner"      # "checks"  | "manual"
    TAB_TERADATA       = "tab_teradata"      # "nonprod" | "prod"
    TAB_BIDSS          = "tab_bidss"         # "query"   | "history"

    # ---- Shared S3 filters (2_Analyse_S3 sidebar) ----
    FLOW_PREFIX        = "flow_prefix"
    FLOW_VERSIONS      = "flow_versions"
    FLOW_DEL_MARKERS   = "flow_delmarkers"
    FLOW_TIME_ENABLED  = "flow_time_enabled"
    FLOW_START_DT      = "flow_start_dt"
    FLOW_END_DT        = "flow_end_dt"
    FLOW_MAX_ITEMS     = "flow_max_items"
    FLOW_CAP_PER_PREFIX = "flow_cap_per_prefix"
    FLOW_DOWNLOAD_DIR  = "flow_download_dir"
    FLOW_PRESERVE      = "flow_preserve"

    # ---- Analyse S3 results / selection ----
    FLOW_S3_RESULTS    = "flow_s3_results"
    FLOW_MAP_SELECTED  = "flow_map_selected_rows"

    # ---- QA tab ----
    QA_MAPPING_DF      = "qa_mapping_df"
    QA_MAP_SELECTED    = "qa_map_selected_rows"
    QA_TESTS_DF        = "qa_tests_df"
    QA_ENTITY_PATHS_DF = "qa_entity_paths_df"
    QA_S3_PATH         = "qa_s3_path"
    QA_SCAN_DF         = "qa_scan_df"
    QA_SCAN_SEL_ROWS   = "qa_scan_selected_rows"
    QA_SELECTED_DF     = "qa_selected_df"

    # ---- App Discovery ----
    AD_MAPPING_DF      = "ad_mapping_df"
    AD_SELECTED_MAPPING = "ad_selected_mapping_df"
    AD_SELECTED_BUCKETS = "ad_selected_buckets"
    AD_MAP_SELECTED    = "ad_map_selected_rows"

    # ---- Upload page ----
    UL_LOCAL_ROOT      = "ul_local_root"
    UL_DEST_PATH       = "ul_dest_path"
    UL_SCAN_DF         = "ul_scan_df"

    # ---- NOS / DynamoDB ----
    NOS_REGION         = "nos_dynamo_region"
    NOS_PIPELINE_ID    = "nos_pipeline_id"

    # ---- Common S3 path (cross-page handoff) ----
    S3_PATH            = "s3_path"
