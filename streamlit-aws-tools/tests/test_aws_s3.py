# tests/test_aws_s3.py
"""
Unit tests for the core modules.

Run with:
    pip install pytest pytest-mock pyarrow pandas boto3 botocore awswrangler
    pytest tests/ -v
"""
from __future__ import annotations

import io
import gzip
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_s3_client_mock(**head_side_effect):
    """Return a minimal boto3 S3 client mock."""
    return MagicMock()


# ---------------------------------------------------------------------------
# src.config
# ---------------------------------------------------------------------------

class TestConfig:
    def test_supported_profiles_not_empty(self):
        from src.config import SUPPORTED_PROFILES
        assert len(SUPPORTED_PROFILES) > 0

    def test_default_region_is_string(self):
        from src.config import DEFAULT_REGION
        assert isinstance(DEFAULT_REGION, str) and DEFAULT_REGION

    def test_extra_mime_types_covers_parquet(self):
        from src.config import EXTRA_MIME_TYPES
        assert ".parquet" in EXTRA_MIME_TYPES

    def test_extra_mime_types_covers_jsonl(self):
        from src.config import EXTRA_MIME_TYPES
        assert ".jsonl" in EXTRA_MIME_TYPES

    def test_sk_has_aws_profile(self):
        from src.config import SK
        assert SK.AWS_PROFILE == "aws_profile"

    def test_sk_has_tab_analyse_s3(self):
        from src.config import SK
        assert hasattr(SK, "TAB_ANALYSE_S3")

    def test_sk_all_values_are_strings(self):
        from src.config import SK
        for attr in vars(SK):
            if not attr.startswith("_"):
                val = getattr(SK, attr)
                assert isinstance(val, str), f"SK.{attr} should be a string, got {type(val)}"


# ---------------------------------------------------------------------------
# src.core.exceptions
# ---------------------------------------------------------------------------

class TestExceptions:
    def test_session_not_ready_is_dpes_error(self):
        from src.core.exceptions import SessionNotReadyError, DPESError
        assert issubclass(SessionNotReadyError, DPESError)

    def test_invalid_profile_is_value_error(self):
        from src.core.exceptions import InvalidProfileError
        assert issubclass(InvalidProfileError, ValueError)

    def test_sso_login_error_is_dpes_error(self):
        from src.core.exceptions import SSOLoginError, DPESError
        assert issubclass(SSOLoginError, DPESError)


# ---------------------------------------------------------------------------
# src.core.common — S3Utils
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# src.core.common — new date-range + file-extension helpers
# ---------------------------------------------------------------------------

class TestGetDefaultDateRange:
    def test_returns_tuple_of_two_datetimes(self):
        from src.core.common import get_default_date_range
        start, end = get_default_date_range()
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)

    def test_end_is_after_start(self):
        from src.core.common import get_default_date_range
        start, end = get_default_date_range()
        assert end > start

    def test_span_is_approximately_one_day(self):
        from src.core.common import get_default_date_range
        start, end = get_default_date_range()
        diff = end - start
        assert timedelta(hours=23, minutes=59) < diff <= timedelta(hours=24, seconds=1)

    def test_both_are_timezone_aware(self):
        from src.core.common import get_default_date_range
        start, end = get_default_date_range()
        assert start.tzinfo is not None
        assert end.tzinfo is not None

    def test_no_microseconds(self):
        from src.core.common import get_default_date_range
        _, end = get_default_date_range()
        assert end.microsecond == 0


class TestExtractFileExtension:
    def test_parquet(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("path/to/file.parquet") == "parquet"

    def test_csv(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("data.csv") == "csv"

    def test_csv_gz(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("data.csv.gz") == "csv.gz"

    def test_json_gz(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("events.json.gz") == "jsonl.gz"

    def test_jsonl_gz(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("events.jsonl.gz") == "jsonl.gz"

    def test_xml_gz(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("feed.xml.gz") == "xml.gz"

    def test_no_extension(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("README") == ""

    def test_empty_string(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("") == ""

    def test_uppercase_is_normalised(self):
        from src.core.common import extract_file_extension
        assert extract_file_extension("DATA.CSV.GZ") == "csv.gz"


# ---------------------------------------------------------------------------
# src.core.common — S3Utils
# ---------------------------------------------------------------------------

class TestS3UtilsParseS3Path:
    def test_full_s3_uri_with_prefix(self):
        from src.core.common import S3Utils
        bucket, prefix = S3Utils.parse_s3_path("s3://my-bucket/some/prefix/")
        assert bucket == "my-bucket"
        assert prefix == "some/prefix/"

    def test_bare_bucket_only(self):
        from src.core.common import S3Utils
        bucket, prefix = S3Utils.parse_s3_path("my-bucket")
        assert bucket == "my-bucket"
        assert prefix == ""

    def test_bucket_slash_prefix(self):
        from src.core.common import S3Utils
        bucket, prefix = S3Utils.parse_s3_path("my-bucket/folder/sub")
        assert bucket == "my-bucket"
        assert prefix == "folder/sub"

    def test_empty_path_raises(self):
        from src.core.common import S3Utils
        with pytest.raises(ValueError):
            S3Utils.parse_s3_path("")

    def test_whitespace_only_raises(self):
        from src.core.common import S3Utils
        with pytest.raises(ValueError):
            S3Utils.parse_s3_path("   ")

    def test_strips_quotes(self):
        from src.core.common import S3Utils
        bucket, prefix = S3Utils.parse_s3_path('"my-bucket/folder"')
        assert bucket == "my-bucket"
        assert prefix == "folder"

    def test_build_s3_uri_no_version(self):
        from src.core.common import S3Utils
        uri = S3Utils.build_s3_uri("bucket", "path/to/file.csv")
        assert uri == "s3://bucket/path/to/file.csv"

    def test_build_s3_uri_with_version(self):
        from src.core.common import S3Utils
        uri = S3Utils.build_s3_uri("bucket", "path/to/file.csv", "abc123")
        assert "versionId=abc123" in uri


class TestS3UtilsSummarizeSubfolders:
    def test_returns_top_level_folders(self):
        from src.core.common import S3Utils
        keys = ["prefix/a/file.csv", "prefix/b/file.csv", "prefix/c.csv"]
        result = S3Utils.summarize_subfolders(keys, "prefix")
        assert result == ["a", "b"]

    def test_returns_empty_when_no_keys(self):
        from src.core.common import S3Utils
        assert S3Utils.summarize_subfolders([], "prefix") == []


class TestPathUtils:
    def test_sanitize_removes_invalid_chars(self):
        from src.core.common import PathUtils
        result = PathUtils.sanitize_component("bad:name*here")
        assert ":" not in result
        assert "*" not in result

    def test_build_local_path_no_structure(self):
        from src.core.common import PathUtils
        result = PathUtils.build_local_path(Path("/tmp"), "prefix/sub/file.csv", "prefix", False)
        assert result.name == "file.csv"

    def test_build_local_path_with_structure(self):
        from src.core.common import PathUtils
        result = PathUtils.build_local_path(Path("/tmp"), "prefix/sub/file.csv", "prefix", True)
        assert "sub" in str(result)


# ---------------------------------------------------------------------------
# src.core.s3_uploader — S3Uploader
# ---------------------------------------------------------------------------

class TestS3UploaderGuessContentType:
    def test_parquet_extension(self):
        from src.core.s3_uploader import S3Uploader
        ct = S3Uploader.guess_content_type(Path("data.parquet"))
        assert ct is not None
        assert "parquet" in ct or "octet" in ct

    def test_jsonl_extension(self):
        from src.core.s3_uploader import S3Uploader
        ct = S3Uploader.guess_content_type(Path("events.jsonl"))
        assert ct is not None

    def test_csv_extension(self):
        from src.core.s3_uploader import S3Uploader
        ct = S3Uploader.guess_content_type(Path("data.csv"))
        assert ct is not None and "csv" in ct


class TestS3UploaderRelativeKey:
    def test_preserve_structure(self):
        from src.core.s3_uploader import S3Uploader
        key = S3Uploader.relative_key(
            Path("/data"), Path("/data/sub/file.csv"), "uploads/", True
        )
        assert key == "uploads/sub/file.csv"

    def test_no_structure(self):
        from src.core.s3_uploader import S3Uploader
        key = S3Uploader.relative_key(
            Path("/data"), Path("/data/sub/file.csv"), "uploads/", False
        )
        assert key == "uploads/file.csv"

    def test_prefix_gets_trailing_slash(self):
        from src.core.s3_uploader import S3Uploader
        key = S3Uploader.relative_key(
            Path("/data"), Path("/data/file.csv"), "uploads", False
        )
        assert key.startswith("uploads/")


class TestS3UploaderFmtSize:
    def test_bytes(self):
        from src.core.s3_uploader import S3Uploader
        assert S3Uploader.fmt_size(512) == "512 B"

    def test_kilobytes(self):
        from src.core.s3_uploader import S3Uploader
        assert "KB" in S3Uploader.fmt_size(2048)

    def test_megabytes(self):
        from src.core.s3_uploader import S3Uploader
        assert "MB" in S3Uploader.fmt_size(5 * 1024 * 1024)


class TestS3UploaderUploadOne:
    def test_skip_when_exists_and_no_overwrite(self, tmp_path):
        from src.core.s3_uploader import S3Uploader
        from boto3.s3.transfer import TransferConfig

        f = tmp_path / "file.txt"
        f.write_text("hello")

        client = MagicMock()
        client.head_object.return_value = {}  # object exists

        uploader = S3Uploader(client)
        ok, msg = uploader.upload_one("bucket", "key", f, None, False, TransferConfig())
        assert ok is False
        assert "Skipped" in (msg or "")
        client.upload_file.assert_not_called()

    def test_upload_called_when_overwrite_true(self, tmp_path):
        from src.core.s3_uploader import S3Uploader
        from boto3.s3.transfer import TransferConfig

        f = tmp_path / "file.txt"
        f.write_text("hello")

        client = MagicMock()
        uploader = S3Uploader(client)
        ok, err = uploader.upload_one("bucket", "key", f, "text/plain", True, TransferConfig())
        assert ok is True
        assert err is None
        client.upload_file.assert_called_once()

    def test_missing_local_file_returns_error(self, tmp_path):
        from src.core.s3_uploader import S3Uploader
        from boto3.s3.transfer import TransferConfig

        client = MagicMock()
        uploader = S3Uploader(client)
        ok, err = uploader.upload_one("bucket", "key", tmp_path / "ghost.csv", None, True, TransferConfig())
        assert ok is False
        assert err is not None


# ---------------------------------------------------------------------------
# src.core.s3_downloader — S3Downloader (download_version uses read() not iter_chunks)
# ---------------------------------------------------------------------------

class TestS3DownloaderVersionedStream:
    def test_reads_in_chunks_without_iter_chunks(self, tmp_path):
        """_download_version must work correctly — boto3 Body has no iter_chunks()."""
        from src.core.s3_downloader import S3Downloader

        content = b"A" * (8 * 1024 * 1024 + 100)  # slightly more than one chunk

        body_mock = MagicMock()
        # Simulate read() returning data then empty bytes
        body_mock.read.side_effect = [content[:8 * 1024 * 1024], content[8 * 1024 * 1024:], b""]
        # Ensure iter_chunks is NOT defined — accessing it should raise AttributeError
        del body_mock.iter_chunks

        client = MagicMock()
        client.get_object.return_value = {"Body": body_mock}

        downloader = S3Downloader(client)
        target = tmp_path / "output.bin"
        saved, err = downloader._download_version("bucket", "key", "v1", target)

        assert err is None
        assert saved == target
        assert target.read_bytes() == content


# ---------------------------------------------------------------------------
# src.core.qa_inspector — QAInspector
# ---------------------------------------------------------------------------

def _make_parquet_bytes() -> bytes:
    """Create a minimal valid Parquet file in memory."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()


def _make_csv_bytes(n: int = 5) -> bytes:
    lines = ["name,age"] + [f"user{i},{20+i}" for i in range(n)]
    return "\n".join(lines).encode()


def _make_jsonl_bytes(n: int = 5) -> bytes:
    lines = [json.dumps({"id": i, "val": f"v{i}"}) for i in range(n)]
    return "\n".join(lines).encode()


class TestQAInspectorGuessType:
    def test_parquet(self):
        from src.core.qa_inspector import QAInspector
        assert QAInspector.guess_type("data.parquet") == "parquet"

    def test_csv(self):
        from src.core.qa_inspector import QAInspector
        assert QAInspector.guess_type("file.csv") == "csv"

    def test_csv_gz(self):
        from src.core.qa_inspector import QAInspector
        assert QAInspector.guess_type("file.csv.gz") == "csv.gz"

    def test_jsonl(self):
        from src.core.qa_inspector import QAInspector
        assert QAInspector.guess_type("events.jsonl") == "jsonl"

    def test_jsonl_gz(self):
        from src.core.qa_inspector import QAInspector
        assert QAInspector.guess_type("events.jsonl.gz") == "jsonl.gz"

    def test_unknown(self):
        from src.core.qa_inspector import QAInspector
        assert QAInspector.guess_type("file.xyz") == "unknown"


class TestQAInspectorParseHelpers:
    """Tests for the private static parsing helpers — no S3 calls needed."""

    def test_parse_head_parquet(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_parquet_bytes()
        df = QAInspector._parse_head(raw, "parquet", 2)
        assert len(df) == 2
        assert list(df.columns) == ["col_a", "col_b"]

    def test_parse_head_csv(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_csv_bytes(10)
        df = QAInspector._parse_head(raw, "csv", 3)
        assert len(df) == 3

    def test_parse_head_jsonl(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_jsonl_bytes(10)
        df = QAInspector._parse_head(raw, "jsonl", 4)
        assert len(df) == 4

    def test_parse_head_csv_gz(self):
        from src.core.qa_inspector import QAInspector
        raw_csv = _make_csv_bytes(5)
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(raw_csv)
        raw = buf.getvalue()
        df = QAInspector._parse_head(raw, "csv.gz", 3)
        assert len(df) == 3

    def test_parse_columns_parquet(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_parquet_bytes()
        cols = QAInspector._parse_columns(raw, "parquet")
        assert cols == ["col_a", "col_b"]

    def test_parse_columns_csv(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_csv_bytes()
        cols = QAInspector._parse_columns(raw, "csv")
        assert cols == ["name", "age"]

    def test_parse_columns_jsonl(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_jsonl_bytes()
        cols = QAInspector._parse_columns(raw, "jsonl")
        assert "id" in cols and "val" in cols

    def test_parse_rowcount_parquet(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_parquet_bytes()
        assert QAInspector._parse_rowcount(raw, "parquet") == 3

    def test_parse_rowcount_csv(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_csv_bytes(7)
        assert QAInspector._parse_rowcount(raw, "csv") == 7

    def test_parse_rowcount_jsonl(self):
        from src.core.qa_inspector import QAInspector
        raw = _make_jsonl_bytes(6)
        assert QAInspector._parse_rowcount(raw, "jsonl") == 6


class TestQAInspectorPublicAPI:
    """Tests for the public API — mock the S3 client."""

    def _make_inspector(self, body_bytes: bytes):
        from src.core.qa_inspector import QAInspector
        client = MagicMock()
        body_mock = MagicMock()
        body_mock.read.return_value = body_bytes
        client.get_object.return_value = {"Body": body_mock}
        return QAInspector(client, MagicMock())

    def test_preview_head_returns_dataframe(self):
        inspector = self._make_inspector(_make_csv_bytes(10))
        df = inspector.preview_head("bucket", "key.csv", "csv", n=5)
        assert len(df) == 5

    def test_list_columns_returns_list(self):
        inspector = self._make_inspector(_make_parquet_bytes())
        cols = inspector.list_columns("bucket", "key.parquet", "parquet")
        assert isinstance(cols, list)
        assert len(cols) == 2

    def test_rowcount_returns_int(self):
        inspector = self._make_inspector(_make_jsonl_bytes(8))
        count = inspector.rowcount("bucket", "key.jsonl", "jsonl")
        assert count == 8

    def test_s3_error_returns_empty(self):
        from botocore.exceptions import ClientError
        from src.core.qa_inspector import QAInspector

        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
        )
        inspector = QAInspector(client, MagicMock())
        df = inspector.preview_head("bucket", "missing.csv", "csv")
        assert df.empty


# ---------------------------------------------------------------------------
# src.aws_s3 — S3SessionManager
# ---------------------------------------------------------------------------

class TestS3SessionManager:
    def test_no_session_raises_session_not_ready(self):
        from src.aws_s3 import S3SessionManager
        from src.core.exceptions import SessionNotReadyError
        mgr = S3SessionManager()
        with pytest.raises(SessionNotReadyError):
            mgr.get_session()

    def test_has_active_session_false_initially(self):
        from src.aws_s3 import S3SessionManager
        mgr = S3SessionManager()
        assert mgr.has_active_session() is False

    def test_invalid_profile_raises(self):
        from src.aws_s3 import S3SessionManager
        from src.core.exceptions import InvalidProfileError
        mgr = S3SessionManager()
        with pytest.raises(InvalidProfileError):
            mgr.login_and_setup(profile="nonexistent-profile", run_sso=False)

    def test_login_sets_active_session(self):
        from src.aws_s3 import S3SessionManager, SUPPORTED_PROFILES

        with patch("boto3.Session") as mock_session_cls, \
             patch.object(S3SessionManager, "_get_s3_endpoint_export", return_value=None), \
             patch.object(S3SessionManager, "_get_identity", return_value={"Account": "123", "Arn": "arn:aws:iam::123:user/test", "UserId": "AIDA123"}):

            mgr = S3SessionManager()
            result = mgr.login_and_setup(
                profile=SUPPORTED_PROFILES[0],
                region="ap-southeast-2",
                run_sso=False,
            )

        assert result["ok"] is True
        assert result["profile"] == SUPPORTED_PROFILES[0]
        assert mgr.has_active_session() is True

    def test_endpoint_override_skips_cf_lookup(self):
        from src.aws_s3 import S3SessionManager, SUPPORTED_PROFILES

        with patch("boto3.Session"), \
             patch.object(S3SessionManager, "_get_s3_endpoint_export") as mock_cf, \
             patch.object(S3SessionManager, "_get_identity", return_value={"Account": "123", "Arn": "arn:aws:iam::123:user/test", "UserId": "AIDA123"}):

            mgr = S3SessionManager()
            result = mgr.login_and_setup(
                profile=SUPPORTED_PROFILES[0],
                run_sso=False,
                s3_endpoint_url_override="https://my-custom-endpoint.example.com",
            )

        mock_cf.assert_not_called()
        assert result["s3_endpoint_url"] == "https://my-custom-endpoint.example.com"

    def test_get_manager_singleton(self):
        # get_manager() must always return the same object
        from src.aws_s3 import get_manager
        m1 = get_manager()
        m2 = get_manager()
        assert m1 is m2


# ---------------------------------------------------------------------------
# src.core.s3_browser — S3Browser
# ---------------------------------------------------------------------------

def _make_paginator(pages: list):
    """Return a minimal paginator mock whose .paginate() returns *pages*."""
    pager = MagicMock()
    pager.paginate.return_value = iter(pages)
    return pager


def _utc(year, month, day, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


class TestS3BrowserLiteralPrefix:
    def test_no_metacharacters(self):
        from src.core.s3_browser import S3Browser
        assert S3Browser._literal_prefix("entity/flight") == "entity/flight"

    def test_dot_star_suffix(self):
        from src.core.s3_browser import S3Browser
        assert S3Browser._literal_prefix("entity/flight.*") == "entity/flight"

    def test_group_metachar(self):
        from src.core.s3_browser import S3Browser
        assert S3Browser._literal_prefix("entity/(flight|train)") == "entity/"

    def test_empty_string(self):
        from src.core.s3_browser import S3Browser
        assert S3Browser._literal_prefix("") == ""

    def test_backslash_dot_extension(self):
        from src.core.s3_browser import S3Browser
        # The backslash is the first special char
        assert S3Browser._literal_prefix("folder/file\\.csv") == "folder/file"


class TestS3BrowserCompilePrefixPattern:
    def test_empty_returns_none(self):
        from src.core.s3_browser import S3Browser
        assert S3Browser._compile_prefix_pattern("") is None

    def test_valid_regex_compiles(self):
        from src.core.s3_browser import S3Browser
        pat = S3Browser._compile_prefix_pattern("entity/flight.*")
        assert pat is not None
        assert pat.search("entity/flightoperations/file.csv")

    def test_invalid_regex_returns_none(self):
        from src.core.s3_browser import S3Browser
        assert S3Browser._compile_prefix_pattern("[invalid") is None


class TestS3BrowserListObjects:
    """list_objects — time filter and regex prefix behaviour."""

    def _make_obj(self, key: str, lm: datetime, size_bytes: int = 1024):
        return {"Key": key, "LastModified": lm, "Size": size_bytes, "StorageClass": "STANDARD"}

    def _browser_with_pages(self, pages):
        client = MagicMock()
        client.get_paginator.return_value = _make_paginator(pages)
        from src.core.s3_browser import S3Browser
        return S3Browser(client)

    # ── time filter removes MaxItems ──────────────────────────────────────────

    def test_no_time_filter_uses_max_items(self):
        """Without a time filter, MaxItems is passed to PaginationConfig."""
        browser = self._browser_with_pages([{"Contents": []}])
        browser.list_objects("bkt", "pfx/", cap=50)
        call_kwargs = browser.s3.get_paginator("list_objects_v2").paginate.call_args[1]
        assert call_kwargs["PaginationConfig"]["MaxItems"] == 50

    def test_time_filter_removes_max_items(self):
        """With a time filter, MaxItems must NOT be in PaginationConfig so all
        pages are scanned before the window is applied."""
        browser = self._browser_with_pages([{"Contents": []}])
        browser.list_objects("bkt", "pfx/", cap=50, start_utc=_utc(2024, 1, 1))
        call_kwargs = browser.s3.get_paginator("list_objects_v2").paginate.call_args[1]
        assert "MaxItems" not in call_kwargs["PaginationConfig"]

    def test_time_filter_objects_beyond_cap_position_are_found(self):
        """Objects that match the time range but sit beyond position *cap* in
        the bucket must be returned when a time filter is active."""
        # Simulate 5 objects: first 3 are outside the window, last 2 are inside
        window_start = _utc(2024, 6, 1)
        objs = [
            self._make_obj(f"obj_{i}.csv", _utc(2024, 1, i + 1)) for i in range(3)
        ] + [
            self._make_obj(f"obj_{i}.csv", _utc(2024, 6, i + 1)) for i in range(3, 5)
        ]
        pages = [{"Contents": objs}]
        browser = self._browser_with_pages(pages)
        rows = browser.list_objects("bkt", "", cap=3, start_utc=window_start)
        # All matching objects are returned even though cap=3 < total objects
        assert len(rows) == 2
        assert all(row["LastModified"] >= window_start for row in rows)

    # ── regex prefix ─────────────────────────────────────────────────────────

    def test_literal_prefix_sent_to_api(self):
        """The S3 API receives only the literal leading portion of the pattern."""
        browser = self._browser_with_pages([{"Contents": []}])
        browser.list_objects("bkt", "entity/flight.*", cap=10)
        call_kwargs = browser.s3.get_paginator("list_objects_v2").paginate.call_args[1]
        assert call_kwargs["Prefix"] == "entity/flight"

    def test_regex_filters_results(self):
        """Only keys matching the full pattern are returned."""
        objs = [
            self._make_obj("entity/flight/data.csv", _utc(2024, 1, 1)),
            self._make_obj("entity/flightoperations/data.csv", _utc(2024, 1, 2)),
            self._make_obj("entity/other/data.csv", _utc(2024, 1, 3)),
        ]
        # Pattern "entity/flight" should match the first two (starts-with via regex search)
        # but NOT "entity/other" (literal prefix would have returned all three)
        browser = self._browser_with_pages([{"Contents": objs}])
        rows = browser.list_objects("bkt", "entity/flight", cap=10)
        keys = [r["Key"] for r in rows]
        assert "entity/flight/data.csv" in keys
        assert "entity/flightoperations/data.csv" in keys
        assert "entity/other/data.csv" not in keys

    def test_regex_dot_star_parquet(self):
        """entity/flight.*\\.parquet only returns parquet files."""
        objs = [
            self._make_obj("entity/flight/2024/data.parquet", _utc(2024, 1, 1)),
            self._make_obj("entity/flight/2024/data.csv", _utc(2024, 1, 2)),
            self._make_obj("entity/flightops/2024/data.parquet", _utc(2024, 1, 3)),
        ]
        browser = self._browser_with_pages([{"Contents": objs}])
        rows = browser.list_objects("bkt", r"entity/flight.*\.parquet", cap=10)
        keys = [r["Key"] for r in rows]
        assert "entity/flight/2024/data.parquet" in keys
        assert "entity/flightops/2024/data.parquet" in keys
        assert "entity/flight/2024/data.csv" not in keys

    def test_results_capped_at_cap(self):
        """Even with time filter active, results are capped at *cap*."""
        window_start = _utc(2024, 1, 1)
        objs = [self._make_obj(f"obj_{i}.csv", _utc(2024, 6, i + 1)) for i in range(20)]
        browser = self._browser_with_pages([{"Contents": objs}])
        rows = browser.list_objects("bkt", "", cap=5, start_utc=window_start)
        assert len(rows) == 5
