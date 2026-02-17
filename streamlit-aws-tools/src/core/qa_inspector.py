# src/core/qa_inspector.py
from __future__ import annotations

import io
import gzip
import json
from typing import Optional, List, Dict

import pandas as pd
import pyarrow.parquet as pq

# Optional dependencies:
try:
    import awswrangler as wr
except Exception:
    wr = None

try:
    import s3fs
except Exception:
    s3fs = None


class QAInspector:
    """
    Version-aware read helpers. For versioned reads, avoid relying on
    wrapper libraries' version support—use `get_object(VersionId=...)`.
    """

    def __init__(self, boto3_client, boto3_session, s3_endpoint_url: Optional[str] = None):
        self.s3 = boto3_client
        self.session = boto3_session
        self.endpoint = s3_endpoint_url

    # ------------- File type detection -------------

    _EXT_MAP = {
        ".parquet": "parquet",
        ".pq": "parquet",
        ".csv": "csv",
        ".json": "jsonl",  # assume NDJSON by default
        ".ndjson": "jsonl",
        ".jsonl": "jsonl",
        ".gz": "gzip",  # refine inner below
    }

    @staticmethod
    def guess_type(key: str) -> str:
        k = key.lower()
        if k.endswith(".csv.gz"):
            return "csv.gz"
        if k.endswith(".json.gz") or k.endswith(".ndjson.gz") or k.endswith(".jsonl.gz"):
            return "jsonl.gz"
        for ext, t in QAInspector._EXT_MAP.items():
            if k.endswith(ext):
                return t
        return "unknown"

    # ------------- Object/object-version IO -------------

    def _get_body_bytes(self, bucket: str, key: str, version_id: Optional[str] = None) -> bytes:
        if version_id:
            resp = self.s3.get_object(Bucket=bucket, Key=key, VersionId=version_id)
        else:
            resp = self.s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    def preview_head(self, bucket: str, key: str, ftype: str, n: int = 10,
                     version_id: Optional[str] = None) -> pd.DataFrame:
        """
        Return at most n rows for a single object or version.
        """
        # Prefer version-aware read using boto3 bytes:
        try:
            raw = self._get_body_bytes(bucket, key, version_id)
            if ftype == "parquet":
                table = pq.read_table(io.BytesIO(raw))
                df = table.to_pandas()
                return df.head(n)

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
                        lines = [next(gz).decode("utf-8") for _ in range(n)]
                        return pd.DataFrame([json.loads(l) for l in lines if l.strip()])
                # uncompressed
                lines = []
                s = io.StringIO(bio.read().decode("utf-8", errors="replace"))
                for _ in range(n):
                    ln = s.readline()
                    if not ln:
                        break
                    lines.append(ln)
                return pd.DataFrame([json.loads(l) for l in lines if l.strip()])

            # fallback try CSV
            bio = io.BytesIO(raw)
            return pd.read_csv(bio, nrows=n)
        except StopIteration:
            return pd.DataFrame()
        except Exception:
            # If bytes path fails for current objects and awswrangler is available, try wr
            if wr and version_id is None:
                try:
                    return wr.s3.read_csv(path=f"s3://{bucket}/{key}", nrows=n, boto3_session=self.session)
                except Exception:
                    return pd.DataFrame()
            return pd.DataFrame()

    def list_columns(self, bucket: str, key: str, ftype: str,
                     version_id: Optional[str] = None) -> List[str]:
        """
        Return ordered list of columns for single file/version.
        """
        try:
            raw = self._get_body_bytes(bucket, key, version_id)
            if ftype == "parquet":
                pf = pq.ParquetFile(io.BytesIO(raw))
                return [n for n in pf.schema.names]
            if ftype in ("csv", "csv.gz"):
                bio = io.BytesIO(raw)
                if ftype.endswith(".gz"):
                    with gzip.GzipFile(fileobj=bio) as gz:
                        df = pd.read_csv(gz, nrows=0)
                        return list(df.columns)
                df = pd.read_csv(bio, nrows=0)
                return list(df.columns)
            if ftype in ("jsonl", "jsonl.gz"):
                # Read first line and take its keys
                bio = io.BytesIO(raw)
                content = None
                if ftype.endswith(".gz"):
                    with gzip.GzipFile(fileobj=bio) as gz:
                        content = gz.readline().decode("utf-8")
                else:
                    content = io.StringIO(bio.read().decode("utf-8", errors="replace")).readline()
                if content:
                    obj = json.loads(content)
                    if isinstance(obj, dict):
                        return list(obj.keys())
                return []
            # fallback: try csv header
            bio = io.BytesIO(raw)
            df = pd.read_csv(bio, nrows=0)
            return list(df.columns)
        except Exception:
            # fallback for current (non-version) via wr
            if wr and version_id is None:
                try:
                    if ftype == "parquet":
                        df = wr.s3.read_parquet(path=f"s3://{bucket}/{key}", dataset=False, boto3_session=self.session)
                        return list(df.columns)
                    if ftype in ("csv", "csv.gz"):
                        df = wr.s3.read_csv(path=f"s3://{bucket}/{key}", nrows=0, boto3_session=self.session)
                        return list(df.columns)
                    if ftype in ("jsonl", "jsonl.gz"):
                        it = wr.s3.read_json(path=f"s3://{bucket}/{key}", lines=True, chunksize=50000,
                                             boto3_session=self.session)
                        try:
                            first = next(it)
                            return list(first.columns)
                        except StopIteration:
                            return []
                except Exception:
                    return []
            return []

    def rowcount(self, bucket: str, key: str, ftype: str,
                 version_id: Optional[str] = None) -> int:
        """
        Count rows. For versions, we stream and count lines where applicable.
        For parquet, we load metadata from bytes (may be heavy for huge files).
        """
        try:
            raw = self._get_body_bytes(bucket, key, version_id)
            if ftype == "parquet":
                pf = pq.ParquetFile(io.BytesIO(raw))
                meta = pf.metadata
                return sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))

            if ftype in ("csv", "csv.gz"):
                if ftype.endswith(".gz"):
                    cnt = 0
                    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
                        for _ in gz:
                            cnt += 1
                    return max(cnt - 1, 0)  # minus header
                # uncompressed
                s = io.StringIO(raw.decode("utf-8", errors="replace"))
                # count lines efficiently
                cnt = sum(1 for _ in s)
                return max(cnt - 1, 0)

            if ftype in ("jsonl", "jsonl.gz"):
                if ftype.endswith(".gz"):
                    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
                        return sum(1 for _ in gz)
                s = io.StringIO(raw.decode("utf-8", errors="replace"))
                return sum(1 for _ in s)

            # fallback: try csv
            s = io.StringIO(raw.decode("utf-8", errors="replace"))
            cnt = sum(1 for _ in s)
            return max(cnt - 1, 0)
        except Exception:
            # fallback for current-only via awswrangler
            if wr and version_id is None:
                try:
                    total = 0
                    for chunk in wr.s3.read_csv(path=f"s3://{bucket}/{key}", chunksize=500_000,
                                                boto3_session=self.session):
                        total += len(chunk)
                    return int(total)
                except Exception:
                    return 0
            return 0