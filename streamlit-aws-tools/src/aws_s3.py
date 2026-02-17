# src/aws_s3.py

from __future__ import annotations

import os
import io
import json
import gzip
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Dict

import boto3
import duckdb  # currently unused, but kept as requested for future ETL work
import pandas as pd  # currently unused
import pyarrow.parquet as pq  # currently unused
import awswrangler as wr
from botocore.config import Config
import xml.etree.ElementTree as ET  # currently unused


DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
DEFAULT_EXPORT_NAME = "S3CustomEndpoint"

SUPPORTED_PROFILES = [
    "a1226-nonprod",
    "a1226-dev",
    "a1226-prod",
]


class S3SessionManager:
    """
    Manages AWS SSO login, boto3 Session creation, and S3 endpoint configuration.
    Designed to be used once from Streamlit homepage, then reused by other pages.
    """

    def __init__(self, default_region: str = DEFAULT_REGION, export_name: str = DEFAULT_EXPORT_NAME):
        self.default_region = default_region
        self.export_name = export_name

        self._active_profile: Optional[str] = None
        self._region: str = default_region
        self._boto3_session: Optional[boto3.Session] = None
        self._s3_endpoint_url: Optional[str] = None
        self._identity: Optional[Dict] = None  # result from sts.get_caller_identity

    # --------------------------
    # Public API
    # --------------------------
    def login_and_setup(
        self,
        profile: str,
        region: Optional[str] = None,
        export_name: Optional[str] = None,
        run_sso: bool = True,
    ) -> Dict:
        """
        Perform AWS SSO login (via CLI), create boto3 Session, fetch S3 custom endpoint (CloudFormation Export),
        configure awswrangler, and validate identity.

        Returns a dict with context info to display in UI.
        """
        region = region or self.default_region
        export_name = export_name or self.export_name

        # 1) Optionally trigger SSO login (opens your default browser)
        if run_sso:
            self._sso_login(profile)

        # 2) Create boto3 Session for the chosen profile/region
        self._boto3_session = boto3.Session(profile_name=profile, region_name=region)
        self._active_profile = profile
        self._region = region

        # 3) Resolve optional custom endpoint from CloudFormation Export
        self._s3_endpoint_url = self._get_s3_endpoint_export(self._boto3_session, export_name)

        # 4) Apply endpoint to awswrangler config (if present)
        if self._s3_endpoint_url:
            wr.config.s3_endpoint_url = self._s3_endpoint_url  # type: ignore[attr-defined]

        # 5) Validate credentials and capture identity
        self._identity = self._get_identity(self._boto3_session)

        return {
            "ok": True,
            "profile": profile,
            "region": region,
            "s3_endpoint_url": self._s3_endpoint_url,
            "identity": self._identity,
        }

    def has_active_session(self) -> bool:
        return self._boto3_session is not None and self._active_profile is not None

    def get_session(self) -> boto3.Session:
        if not self._boto3_session:
            raise RuntimeError("No active boto3 Session. Run login first on the homepage.")
        return self._boto3_session

    def get_s3_client(self):
        """
        Returns an S3 client bound to the resolved endpoint (if any) and a resilient retry config.
        """
        session = self.get_session()
        cfg = Config(retries={"max_attempts": 5, "mode": "standard"})
        return session.client("s3", config=cfg, endpoint_url=self._s3_endpoint_url)

    def get_s3_resource(self):
        session = self.get_session()
        return session.resource("s3", endpoint_url=self._s3_endpoint_url)

    def get_client(self, service_name: str):
        """
        Generic client getter for other AWS services (e.g., 'cloudformation', 'sts').
        Note: Only S3 gets the custom endpoint.
        """
        session = self.get_session()
        if service_name == "s3":
            return self.get_s3_client()
        return session.client(service_name)

    def current_context(self) -> Dict:
        """
        Introspect current session context (for UI display).
        """
        return {
            "profile": self._active_profile,
            "region": self._region,
            "s3_endpoint_url": self._s3_endpoint_url,
            "identity": self._identity,
        }

    # --------------------------
    # Internals
    # --------------------------
    def _sso_login(self, profile: str) -> None:
        """
        Calls 'aws sso login --profile <profile>'.
        This will open a browser window on the machine running Streamlit.
        """
        # Locate AWS CLI
        aws_exe = shutil.which("aws") or shutil.which("aws.exe") or shutil.which("aws.cmd")
        if not aws_exe:
            raise RuntimeError(
                "AWS CLI not found. Please install & configure AWS CLI v2 and retry.\n"
                "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
            )

        try:
            subprocess.run(
                [aws_exe, "sso", "login", "--profile", profile],
                check=True,
                capture_output=False,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"AWS SSO login failed for profile '{profile}'.\n"
                f"Exit code: {e.returncode}"
            ) from e

    def _get_s3_endpoint_export(self, session: boto3.Session, export_name: str) -> Optional[str]:
        """
        Scans CloudFormation exports (paginated) for a given export name and returns its Value (URL).
        If not found, returns None (we'll fall back to the standard S3 endpoint).
        """
        cf = session.client("cloudformation")
        next_token = None
        while True:
            if next_token:
                resp = cf.list_exports(NextToken=next_token)
            else:
                resp = cf.list_exports()

            for export in resp.get("Exports", []):
                if export.get("Name") == export_name:
                    return export.get("Value")

            next_token = resp.get("NextToken")
            if not next_token:
                break

        return None

    def _get_identity(self, session: boto3.Session) -> Dict:
        sts = session.client("sts")
        return sts.get_caller_identity()


# -------------
# Singleton helper (nice for Streamlit)
# -------------
_manager_singleton: Optional[S3SessionManager] = None


def get_manager() -> S3SessionManager:
    global _manager_singleton
    if _manager_singleton is None:
        _manager_singleton = S3SessionManager()
    return _manager_singleton