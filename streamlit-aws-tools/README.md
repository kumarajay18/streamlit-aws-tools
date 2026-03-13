# DPES AWS Tools — Streamlit Dashboard

A multi-page **Streamlit** web application for the DPES (Distributed Platform Engineering Services) team, providing AWS S3 management, ETL quality-assurance, Teradata SQL workbench, Lambda analysis, SQS messaging, and Splunk log exploration — all in one internal dashboard.

---

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the App](#running-the-app)
- [AWS Authentication](#aws-authentication)
- [Configuration](#configuration)
- [Running Tests](#running-tests)
- [Architecture & OOP Design](#architecture--oop-design)
- [Module Reference](#module-reference)
- [Known Limitations](#known-limitations)

---

## Features

| Tool | Description |
|------|-------------|
| **App Discovery** | Scan S3 for CloudFormation packaged templates |
| **Analyse S3 / ETL QA** | Browse, inspect (Parquet/CSV/JSONL), download & delete S3 objects |
| **Analyse Lambda** | List Lambda functions, read environment variables and CloudWatch logs |
| **Upload Files** | Upload local files or entire folder trees to S3 |
| **Teradata SQL** | JDBC workbench against NonProd & Prod Teradata databases |
| **Send SQS** | Send messages to standard or FIFO SQS queues |
| **BIDSS / PQMF** | Teradata workbench for PQMF database queries |
| **Modify NOS Table** | Edit TeradataNosIntegration DynamoDB table entries |
| **Splunk Logs** | Query Splunk ETL trace logs with configurable index & source type |

---

## Project Structure

```
streamlit-aws-tools/
├── app.py                     # Home page — AWS SSO login & navigation
├── requirements.txt           # Pinned Python dependencies
├── .gitignore
│
├── assets/
│   ├── qantas_logo.png
│   └── qantas_logo_v.png
│
├── pages/                     # Streamlit multi-page app pages
│   ├── 0_App_Discovery.py
│   ├── 2_Analyse_S3.py
│   ├── 3_Analyse_Lambda.py
│   ├── 3_Upload_Files.py
│   ├── 5_Teradata_SQL.py
│   ├── 6_Send_SQS.py
│   ├── 8_BIDSS.py
│   ├── 9_Modify_NOS_Table.py
│   └── 10_Splunk_logs.py
│
├── sql/                       # Pre-built Teradata SQL query library
│   ├── registry.py            # Query registry + cached SQL loader
│   └── *.sql                  # Individual query files
│
├── src/                       # Core application source code
│   ├── config.py              # Central constants (region, profiles, limits, etc.)
│   ├── aws_s3.py              # S3SessionManager — AWS SSO login & session lifecycle
│   │
│   ├── core/
│   │   ├── exceptions.py      # Custom exception hierarchy
│   │   ├── common.py          # S3Utils, PathUtils — path parsing & sanitisation
│   │   ├── s3_browser.py      # List objects & versions
│   │   ├── s3_downloader.py   # Download current objects or specific versions
│   │   ├── s3_uploader.py     # Upload files / folder trees
│   │   ├── s3_deleter.py      # Batch delete objects or versions
│   │   └── qa_inspector.py    # Read & inspect Parquet / CSV / JSONL files
│   │
│   └── ui/
│       ├── state.py           # Streamlit session-state helpers
│       └── topbar.py          # Sticky navigation bar component
│
└── tests/
    └── test_aws_s3.py         # Unit tests (pytest)
```

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python | 3.11 or later |
| AWS CLI v2 | Latest |
| pip | 23+ |

Configure your AWS profiles in `~/.aws/config` before running the app:

```ini
[profile a1226-nonprod]
sso_start_url  = https://your-org.awsapps.com/start
sso_region     = ap-southeast-2
sso_account_id = 123456789012
sso_role_name  = YourRoleName
region         = ap-southeast-2
```

---

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd streamlit-aws-tools

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Running the App

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`.

> **Tip:** On first run, select your AWS profile in the sidebar and click **Login with AWS SSO**. This opens a browser tab to complete SSO authentication. Once logged in, all pages share the same session — no need to log in again per page.

---

## AWS Authentication

Authentication is handled by `S3SessionManager` in `src/aws_s3.py`:

1. **Login with AWS SSO** — calls `aws sso login --profile <profile>` in a subprocess (opens browser).
2. **Reuse Existing Session** — creates a boto3 Session from cached SSO credentials (no browser).
3. **Endpoint Override** — optionally point all S3 calls to a custom endpoint (e.g. VPC endpoint or S3-compatible service). If not provided, the app looks for a CloudFormation export named `S3CustomEndpoint`.

Session state is stored in Streamlit's `st.session_state` and shared across pages via the singleton `get_manager()`.

---

## Configuration

All tunable constants live in **`src/config.py`**:

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_REGION` | `ap-southeast-2` | AWS region (overridable via `AWS_DEFAULT_REGION` env var) |
| `DEFAULT_EXPORT_NAME` | `S3CustomEndpoint` | CloudFormation export for custom S3 endpoint |
| `SUPPORTED_PROFILES` | `[a1226-nonprod, a1226-dev, a1226-prod]` | Allowed AWS CLI profile names |
| `LIST_CAP_DEFAULT` | `1000` | Default cap for S3 object listing |
| `LIST_CAP_MAX` | `50000` | Maximum cap the UI allows |
| `DOWNLOAD_CHUNK_SIZE` | `8 MB` | Chunk size for streaming versioned S3 downloads |
| `QA_LIST_CAP` | `5000` | Max objects inspected in a single ETL QA run |
| `EXTRA_MIME_TYPES` | See config | MIME types for `.parquet`, `.jsonl`, `.sql`, etc. |

You can also set environment variables in a `.env` file (loaded by `python-dotenv`):

```dotenv
AWS_DEFAULT_REGION=ap-southeast-2
S3_ENDPOINT_EXPORT_NAME=S3CustomEndpoint
```

---

## Running Tests

```bash
pip install pytest pytest-mock pyarrow
pytest tests/ -v
```

The test suite covers:

- `src/config.py` — constants sanity checks
- `src/core/exceptions.py` — exception hierarchy
- `src/core/common.py` — S3 path parsing, subfolder summarisation, path sanitisation
- `src/core/s3_uploader.py` — MIME guessing, key building, upload logic
- `src/core/s3_downloader.py` — versioned streaming (verifies `iter_chunks` bug is fixed)
- `src/core/qa_inspector.py` — file-type detection, Parquet/CSV/JSONL parsing, S3 error handling
- `src/aws_s3.py` — session lifecycle, profile validation, endpoint override

---

## Architecture & OOP Design

### Class Hierarchy

```
DPESError (base)
├── SessionNotReadyError
├── SSOLoginError
├── InvalidProfileError (also ValueError)
├── S3OperationError
│   ├── S3DownloadError
│   ├── S3UploadError
│   ├── S3DeleteError
│   └── S3BrowseError
├── InvalidS3PathError (also ValueError)
└── UnsupportedFileTypeError (also ValueError)
```

### Key Classes

| Class | File | Responsibility |
|-------|------|----------------|
| `S3SessionManager` | `src/aws_s3.py` | Single owner of boto3 Session; SSO login; endpoint resolution |
| `S3Browser` | `src/core/s3_browser.py` | List current objects and all versions with time-window filtering |
| `S3Downloader` | `src/core/s3_downloader.py` | Download current objects (multipart) or specific versions (streamed) |
| `S3Uploader` | `src/core/s3_uploader.py` | Upload files/trees; MIME detection; overwrite guard |
| `S3Deleter` | `src/core/s3_deleter.py` | Batch delete objects or versions; collects per-item errors |
| `QAInspector` | `src/core/qa_inspector.py` | Read Parquet/CSV/JSONL; column listing; row counting |
| `S3Utils` | `src/core/common.py` | Static S3 path parsing, URI building, subfolder summarisation |
| `PathUtils` | `src/core/common.py` | Static Windows-safe local path building |

### Design Principles

- **Dependency Injection** — every core class receives a `boto3_client` in `__init__`; no global client references.
- **Single Responsibility** — each class does one thing (browse / download / upload / delete / inspect).
- **Fail Fast with Context** — custom exceptions carry descriptive messages; `ClientError` is caught and re-raised with the AWS error code where appropriate.
- **Separation of Concerns** — UI logic lives in `pages/` and `src/ui/`; AWS logic lives in `src/core/`; constants live in `src/config.py`.
- **Backward Compatibility** — `src/aws_s3.py` re-exports `DEFAULT_REGION`, `DEFAULT_EXPORT_NAME`, `SUPPORTED_PROFILES` so existing page imports remain unchanged.

---

## Module Reference

### `src/config.py`
Central home for all tunable constants.  Import from here instead of hard-coding values in page files.

### `src/aws_s3.py`
`S3SessionManager` manages the full lifecycle of an AWS session:
- `login_and_setup(profile, region, run_sso, s3_endpoint_url_override)` — authenticate and configure
- `get_session()` → `boto3.Session`
- `get_s3_client()` → boto3 S3 client (with retry config and custom endpoint)
- `get_client(service)` → boto3 client for any other service
- `has_active_session()`, `current_context()` — introspection helpers
- `get_manager()` — process-level singleton for use across Streamlit pages

### `src/core/exceptions.py`
Custom exception hierarchy rooted at `DPESError`.  Use specific subclasses in `except` blocks for precise error handling.

### `src/core/common.py`
- `S3Utils.parse_s3_path(s3_path)` — accepts `s3://bucket/prefix`, bare `bucket/prefix`, or ARNs
- `S3Utils.build_s3_uri(bucket, key, version_id?)` — builds canonical S3 URI
- `S3Utils.to_utc(dt)` — normalises datetimes to UTC-aware
- `PathUtils.build_local_path(dest_dir, key, base_prefix, preserve_structure)` — Windows-safe local path
- `PathUtils.sanitize_component(name)` — strips Windows-invalid characters

### `src/core/s3_browser.py`
- `list_objects(bucket, prefix, cap, start_utc?, end_utc?)` — paginated list with optional time window
- `list_object_versions(bucket, prefix, cap, ...)` — includes delete markers
- `find_latest_object(bucket, prefix, ...)` — returns the most recently modified object

### `src/core/s3_downloader.py`
- `download_one(bucket, key, dest_dir, base_prefix, preserve_structure, version_id?)` — single file
- `download_many(bucket, items, dest_dir, base_prefix, preserve_structure)` — batch download

### `src/core/s3_uploader.py`
- `iter_local_files(root)` — generator of all files under a path
- `relative_key(local_root, file_path, dest_prefix, preserve_structure)` — compute S3 key
- `guess_content_type(file_path)` — MIME type (includes custom map for `.parquet`, `.jsonl`)
- `upload_one(bucket, key, file_path, content_type, overwrite, transfer_cfg, sse?, kms_key_id?)` — single upload
- `fmt_size(nbytes)` — human-readable file size

### `src/core/s3_deleter.py`
- `delete_current(bucket, keys)` — places delete markers on versioned buckets
- `delete_versions(bucket, items)` — permanently deletes specific versions or delete markers

### `src/core/qa_inspector.py`
- `guess_type(key)` — infers format from extension
- `preview_head(bucket, key, ftype, n, version_id?)` → `pd.DataFrame`
- `list_columns(bucket, key, ftype, version_id?)` → `List[str]`
- `rowcount(bucket, key, ftype, version_id?)` → `int`

### `sql/registry.py`
- `load_sql(file_name)` — cached SQL file loader
- `QUERY_REGISTRY` — dict mapping display names to `{category, file, params, help}`

---

## Known Limitations

- **SSO login opens a browser on the Streamlit server machine.** Running on a headless remote server requires a workaround (e.g. pre-authenticate with `aws sso login` in a terminal before starting the app).
- **awswrangler endpoint config is process-global.** If you switch profiles with different S3 endpoints in the same Streamlit session, awswrangler's `wr.config.s3_endpoint_url` will be updated. This is fine for single-profile workflows.
- **Teradata JDBC** pages (`5_Teradata_SQL.py`, `8_BIDSS.py`) require a Teradata JDBC driver on the classpath and network access to Teradata hosts.
