# src/aws_s3.py

from __future__ import annotations

import shutil
import subprocess
from typing import Optional, Dict

import boto3
import awswrangler as wr
from botocore.config import Config

from src.config import DEFAULT_REGION, DEFAULT_EXPORT_NAME, SUPPORTED_PROFILES
from src.core.exceptions import SessionNotReadyError, SSOLoginError, InvalidProfileError

# Re-export for backward compatibility with modules that import these from aws_s3
__all__ = [
    "S3SessionManager",
    "get_manager",
    "DEFAULT_REGION",
    "DEFAULT_EXPORT_NAME",
    "SUPPORTED_PROFILES",
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
        s3_endpoint_url_override: Optional[str] = None,
    ) -> Dict:
        """
        Perform AWS SSO login (via CLI), create boto3 Session, fetch S3 custom endpoint (CloudFormation Export),
        configure awswrangler, and validate identity.

        Args:
            profile: AWS CLI profile name (must exist in ~/.aws/config).
            region: AWS region (falls back to default_region).
            export_name: CloudFormation export name to resolve a custom S3 endpoint URL.
            run_sso: When True, invoke ``aws sso login`` in a subprocess (opens a browser window).
            s3_endpoint_url_override: Explicit S3 endpoint URL; if provided, skips the CloudFormation
                export lookup entirely.

        Returns:
            A dict with context info: ok, profile, region, s3_endpoint_url, identity.
        """
        region = region or self.default_region
        export_name = export_name or self.export_name

        if not profile or profile not in SUPPORTED_PROFILES:
            raise InvalidProfileError(
                f"Profile '{profile}' is not in SUPPORTED_PROFILES: {SUPPORTED_PROFILES}"
            )
        if not region:
            raise ValueError("AWS region must not be empty.")

        # 1) Optionally trigger SSO login (opens your default browser)
        if run_sso:
            self._sso_login(profile)

        # 2) Create boto3 Session for the chosen profile/region
        self._boto3_session = boto3.Session(profile_name=profile, region_name=region)
        self._active_profile = profile
        self._region = region

        # 3) Resolve optional custom endpoint:
        #    - Use explicit override first, then fall back to CloudFormation export lookup.
        if s3_endpoint_url_override:
            self._s3_endpoint_url = s3_endpoint_url_override
        else:
            self._s3_endpoint_url = self._get_s3_endpoint_export(self._boto3_session, export_name)

        # 4) Apply endpoint to awswrangler config (session-level, not process-global where avoidable).
        #    Note: awswrangler reads wr.config.s3_endpoint_url at call time, so updating it here
        #    affects all subsequent wr.s3.* calls in this process. This is acceptable for a
        #    single-profile Streamlit app; if you need multi-endpoint support, pass
        #    boto3_session with endpoint configured instead.
        if self._s3_endpoint_url:
            wr.config.s3_endpoint_url = self._s3_endpoint_url  # type: ignore[attr-defined]
        else:
            wr.config.s3_endpoint_url = None  # type: ignore[attr-defined]

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
            raise SessionNotReadyError(
                "No active boto3 Session. Go to the Home page and log in first."
            )
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
        aws_exe = shutil.which("aws") or shutil.which("aws.exe") or shutil.which("aws.cmd")
        if not aws_exe:
            raise SSOLoginError(
                "AWS CLI not found. Please install & configure AWS CLI v2 and retry.\n"
                "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
            )

        try:
            subprocess.run(
                [aws_exe, "sso", "login", "--profile", profile],
                check=True,
                capture_output=False,
                timeout=300,  # 5-minute timeout; SSO login requires a browser interaction
            )
        except subprocess.TimeoutExpired as e:
            raise SSOLoginError(
                f"AWS SSO login timed out for profile '{profile}'. "
                "Complete the browser authentication within 5 minutes."
            ) from e
        except subprocess.CalledProcessError as e:
            raise SSOLoginError(
                f"AWS SSO login failed for profile '{profile}'. Exit code: {e.returncode}"
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