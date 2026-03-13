# src/core/qa_inspector.py
from __future__ import annotations

import io
import gzip
import json
import logging
from typing import Optional, List

import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Optional dependencies — degrade gracefully if not installed
try:
    import awswrangler as wr
except ImportError:
    wr = None  # type: ignore[assignment]

try:
    import s3fs  # noqa: F401 — imported for side-effects (registers fsspec protocol)
except ImportError:
    pass


class QAInspector:
    """
    Version-aware read helpers for S3 objects.

    For versioned reads, all operations go through ``get_object(VersionId=...)``
    directly (boto3) rather than through wrapper libraries, because most high-level
    libraries do not support the ``VersionId`` parameter natively.
    """

    # --- File-type detection map -------------------------------------------
    _EXT_MAP = {
        ".parquet": "parquet",
        ".pq": "parquet",
        ".csv": "csv",
        ".json": "jsonl",    # assume NDJSON by default
        ".ndjson": "jsonl",
        ".jsonl": "jsonl",
        ".gz": "gzip",       # compound extension handling below
    }

    def __init__(self, boto3_client, boto3_session, s3_endpoint_url: Optional[str] = None):
        self.s3 = boto3_client
        self.session = boto3_session
        self.endpoint = s3_endpoint_url

    # -----------------------------------------------------------------------
    # File-type detection
    # -----------------------------------------------------------------------

    @staticmethod
    def guess_type(key: str) -> str:
        """Guess the data format from the S3 object key's extension."""
        k = key.lower()
        if k.endswith(".csv.gz"):
            return "csv.gz"
        if k.endswith(".json.gz") or k.endswith(".ndjson.gz") or k.endswith(".jsonl.gz"):
            return "jsonl.gz"
        for ext, t in QAInspector._EXT_MAP.items():
            if k.endswith(ext):
                return t
        return "unknown"

    # -----------------------------------------------------------------------
    # Low-level I/O
    # -----------------------------------------------------------------------

    def _get_body_bytes(self, bucket: str, key: str, version_id: Optional[str] = None) -> bytes:
        """Fetch the full body of an S3 object (or specific version) as bytes."""
        kwargs = {"Bucket": bucket, "Key": key}
        if version_id:
            kwargs["VersionId"] = version_id
        resp = self.s3.get_object(**kwargs)
        return resp["Body"].read()

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def preview_head(
        self,
        bucket: str,
        key: str,
        ftype: str,
        n: int = 10,
        version_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Return at most *n* rows from an S3 object (or a specific version).

        Falls back to awswrangler for current (non-versioned) objects when the
        primary byte-based path fails, provided ``awswrangler`` is installed.

        Returns an empty ``DataFrame`` when the object cannot be read.
        """
        try:
            raw = self._get_body_bytes(bucket, key, version_id)
        except ClientError as exc:
            logger.warning("preview_head: could not fetch %s/%s (v=%s): %s", bucket, key, version_id, exc)
            return pd.DataFrame()

        try:
            return self._parse_head(raw, ftype, n)
        except Exception:
            # Primary parse failed — try awswrangler for non-versioned current objects
            if wr is not None and version_id is None:
                try:
                    return wr.s3.read_csv(
                        path=f"s3://{bucket}/{key}", nrows=n, boto3_session=self.session
                    )
                except Exception:
                    pass
            logger.debug("preview_head: could not parse %s (ftype=%s)", key, ftype)
            return pd.DataFrame()

    def list_columns(
        self,
        bucket: str,
        key: str,
        ftype: str,
        version_id: Optional[str] = None,
    ) -> List[str]:
        """
        Return the ordered column names for a single file / version.

        Returns an empty list when the object cannot be read or parsed.
        """
        try:
            raw = self._get_body_bytes(bucket, key, version_id)
        except ClientError as exc:
            logger.warning("list_columns: could not fetch %s/%s: %s", bucket, key, exc)
            return []

        try:
            return self._parse_columns(raw, ftype)
        except Exception:
            if wr is not None and version_id is None:
                try:
                    return self._wr_columns(bucket, key, ftype)
                except Exception:
                    pass
            logger.debug("list_columns: could not parse columns for %s (ftype=%s)", key, ftype)
            return []

    def rowcount(
        self,
        bucket: str,
        key: str,
        ftype: str,
        version_id: Optional[str] = None,
    ) -> int:
        """
        Count rows in a single file / version.

        For Parquet this reads only row-group metadata (fast).
        For CSV / JSONL this streams line-by-line.

        Returns 0 on any error.
        """
        try:
            raw = self._get_body_bytes(bucket, key, version_id)
        except ClientError as exc:
            logger.warning("rowcount: could not fetch %s/%s: %s", bucket, key, exc)
            return 0

        try:
            return self._parse_rowcount(raw, ftype)
        except Exception:
            if wr is not None and version_id is None:
                try:
                    total = 0
                    for chunk in wr.s3.read_csv(
                        path=f"s3://{bucket}/{key}", chunksize=500_000, boto3_session=self.session
                    ):
                        total += len(chunk)
                    return int(total)
                except Exception:
                    pass
            logger.debug("rowcount: could not count rows for %s (ftype=%s)", key, ftype)
            return 0

    # -----------------------------------------------------------------------
    # Private parsing helpers (operate on in-memory bytes)
    # -----------------------------------------------------------------------

    @staticmethod
    def _parse_head(raw: bytes, ftype: str, n: int) -> pd.DataFrame:
        if ftype == "parquet":
            table = pq.read_table(io.BytesIO(raw))
            return table.to_pandas().head(n)

        if ftype in ("csv", "csv.gz"):
            bio = io.BytesIO(raw)
            if ftype.endswith(".gz"):
                with gzip.GzipFile(fileobj=bio) as gz:
                    return pd.read_csv(gz, nrows=n)
            return pd.read_csv(bio, nrows=n)

        if ftype in ("jsonl", "jsonl.gz"):
            bio = io.BytesIO(raw)
            if ftype.endswith(".gz"):
                with gzip.GzipFile(fileobj=bio) as gz:
                    lines = []
                    for _ in range(n):
                        line = gz.readline()
                        if not line:
                            break
                        lines.append(line.decode("utf-8"))
                    return pd.DataFrame([json.loads(l) for l in lines if l.strip()])
            text = bio.read().decode("utf-8", errors="replace")
            lines = []
            for line in io.StringIO(text):
                if len(lines) >= n:
                    break
                lines.append(line)
            return pd.DataFrame([json.loads(l) for l in lines if l.strip()])

        # Unknown type — attempt CSV as best-effort
        return pd.read_csv(io.BytesIO(raw), nrows=n)

    @staticmethod
    def _parse_columns(raw: bytes, ftype: str) -> List[str]:
        if ftype == "parquet":
            pf = pq.ParquetFile(io.BytesIO(raw))
            return list(pf.schema.names)

        if ftype in ("csv", "csv.gz"):
            bio = io.BytesIO(raw)
            if ftype.endswith(".gz"):
                with gzip.GzipFile(fileobj=bio) as gz:
                    return list(pd.read_csv(gz, nrows=0).columns)
            return list(pd.read_csv(bio, nrows=0).columns)

        if ftype in ("jsonl", "jsonl.gz"):
            bio = io.BytesIO(raw)
            if ftype.endswith(".gz"):
                with gzip.GzipFile(fileobj=bio) as gz:
                    first_line = gz.readline().decode("utf-8")
            else:
                first_line = io.StringIO(
                    bio.read().decode("utf-8", errors="replace")
                ).readline()
            if first_line.strip():
                obj = json.loads(first_line)
                if isinstance(obj, dict):
                    return list(obj.keys())
            return []

        # Fallback: try CSV header
        return list(pd.read_csv(io.BytesIO(raw), nrows=0).columns)

    @staticmethod
    def _parse_rowcount(raw: bytes, ftype: str) -> int:
        if ftype == "parquet":
            pf = pq.ParquetFile(io.BytesIO(raw))
            meta = pf.metadata
            return sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))

        if ftype in ("csv", "csv.gz"):
            if ftype.endswith(".gz"):
                cnt = sum(1 for _ in gzip.GzipFile(fileobj=io.BytesIO(raw)))
            else:
                cnt = sum(1 for _ in io.StringIO(raw.decode("utf-8", errors="replace")))
            return max(cnt - 1, 0)  # subtract header row

        if ftype in ("jsonl", "jsonl.gz"):
            if ftype.endswith(".gz"):
                return sum(1 for _ in gzip.GzipFile(fileobj=io.BytesIO(raw)))
            return sum(1 for _ in io.StringIO(raw.decode("utf-8", errors="replace")))

        # Fallback: treat as CSV
        cnt = sum(1 for _ in io.StringIO(raw.decode("utf-8", errors="replace")))
        return max(cnt - 1, 0)

    def _wr_columns(self, bucket: str, key: str, ftype: str) -> List[str]:
        """awswrangler-based fallback for column listing (current objects only)."""
        if wr is None:
            raise ImportError(
                "awswrangler is required for this fallback but is not installed. "
                "Run: pip install awswrangler"
            )
        s3_path = f"s3://{bucket}/{key}"
        if ftype == "parquet":
            df = wr.s3.read_parquet(path=s3_path, dataset=False, boto3_session=self.session)
            return list(df.columns)
        if ftype in ("csv", "csv.gz"):
            df = wr.s3.read_csv(path=s3_path, nrows=0, boto3_session=self.session)
            return list(df.columns)
        if ftype in ("jsonl", "jsonl.gz"):
            it = wr.s3.read_json(path=s3_path, lines=True, chunksize=50_000, boto3_session=self.session)
            first = next(it)
            return list(first.columns)
        return []
