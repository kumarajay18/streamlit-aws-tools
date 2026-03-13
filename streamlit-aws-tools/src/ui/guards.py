# src/ui/guards.py
"""
Page-entry guard helpers.

Every page in the app requires an active AWS session before it can do anything
useful.  Previously, each page had its own copy of this three-line pattern::

    mgr = get_manager()
    if not mgr.has_active_session():
        st.warning("No active AWS session…")
        st.stop()

This module replaces that repetition with a single call.

Usage::

    from src.ui.guards import require_aws_session
    mgr = require_aws_session()          # halts the page if not logged in
    s3  = mgr.get_s3_client()
"""
from __future__ import annotations

import streamlit as st

from src.aws_s3 import get_manager, S3SessionManager


def require_aws_session(message: str | None = None) -> S3SessionManager:
    """
    Ensure an active AWS session exists; halt the Streamlit page if not.

    Args:
        message: Optional custom warning text.  Defaults to a standard message
                 directing the user to the Home page.

    Returns:
        The active :class:`S3SessionManager` singleton.

    Raises:
        ``st.stop()`` is called (not raised) if no session is active, so this
        function never returns in that case — Streamlit stops rendering the page.
    """
    mgr = get_manager()
    if not mgr.has_active_session():
        st.warning(
            message
            or (
                "⚠️ No active AWS session. "
                "Go to the **Home** page and click **Login with AWS SSO** "
                "(or **Reuse Existing Session**) first."
            )
        )
        st.stop()
    return mgr
