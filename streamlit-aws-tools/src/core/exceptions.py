# src/core/exceptions.py
"""
Custom exception hierarchy for the DPES AWS Tools application.

Having a dedicated exception hierarchy (instead of bare ``Exception`` or
``RuntimeError``) makes it easy to:

* catch only the errors you expect (tight ``except`` blocks)
* re-raise with enriched context
* distinguish user errors (bad input) from infrastructure errors (AWS outages)

Usage::

    from src.core.exceptions import S3OperationError, SessionNotReadyError

    try:
        downloader.download_one(...)
    except S3OperationError as exc:
        st.error(str(exc))
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class DPESError(Exception):
    """Root exception for all DPES AWS Tools errors."""


# ---------------------------------------------------------------------------
# Session / Auth errors
# ---------------------------------------------------------------------------

class SessionNotReadyError(DPESError):
    """Raised when an S3 operation is attempted before a boto3 session exists."""


class SSOLoginError(DPESError):
    """Raised when ``aws sso login`` subprocess fails."""


class InvalidProfileError(DPESError, ValueError):
    """Raised when the requested AWS profile is not in SUPPORTED_PROFILES."""


# ---------------------------------------------------------------------------
# S3 operation errors
# ---------------------------------------------------------------------------

class S3OperationError(DPESError):
    """Base class for errors that occur during S3 API calls."""


class S3DownloadError(S3OperationError):
    """Raised when an S3 object (or version) cannot be downloaded."""


class S3UploadError(S3OperationError):
    """Raised when an S3 file upload fails."""


class S3DeleteError(S3OperationError):
    """Raised when an S3 delete operation reports errors."""


class S3BrowseError(S3OperationError):
    """Raised when listing S3 objects or versions fails."""


# ---------------------------------------------------------------------------
# Parsing / validation errors
# ---------------------------------------------------------------------------

class InvalidS3PathError(DPESError, ValueError):
    """Raised when an S3 path string cannot be parsed into bucket + prefix."""


class UnsupportedFileTypeError(DPESError, ValueError):
    """Raised when QAInspector is asked to inspect an unsupported file format."""
