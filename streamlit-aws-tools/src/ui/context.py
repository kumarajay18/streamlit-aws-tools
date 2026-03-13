# src/ui/context.py
"""
Session-context display helpers.

Every page that uses AWS services shows the same one-liner::

    st.caption(f"Using profile **{ctx.get('profile')}**, region **{ctx.get('region')}**. "
               f"S3 endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**")

This module centralises that pattern so it only needs to be maintained in one
place.

Usage::

    from src.ui.context import show_session_caption
    ctx = show_session_caption()          # renders the caption and returns ctx dict
    s3  = mgr.get_s3_client()
"""
from __future__ import annotations

from typing import Dict, Optional

import streamlit as st

from src.aws_s3 import get_manager
from src.config import DEFAULT_REGION


def show_session_caption(
    region_override: Optional[str] = None,
    show_endpoint: bool = True,
    extra_note: Optional[str] = None,
) -> Dict:
    """
    Render a one-line ``st.caption`` summarising the current AWS session, then
    return the raw context dict.

    Args:
        region_override: If provided, use this region string in the caption
                         instead of the one stored in the context (useful for
                         pages that force a specific region).
        show_endpoint:   When ``False``, omit the S3-endpoint part of the caption
                         (handy for pages unrelated to S3, e.g. SQS, Lambda).
        extra_note:      Optional suffix appended inside the caption, e.g. to
                         note that the endpoint is irrelevant for that service.

    Returns:
        The ``current_context()`` dict from :class:`S3SessionManager`.
    """
    mgr = get_manager()
    ctx = mgr.current_context() if mgr.has_active_session() else {}

    profile = ctx.get("profile") or "—"
    region = region_override or ctx.get("region") or DEFAULT_REGION
    endpoint = ctx.get("s3_endpoint_url") or "standard"

    if show_endpoint:
        note = f"  {extra_note}" if extra_note else ""
        st.caption(
            f"Using profile **{profile}**, region **{region}**. "
            f"S3 endpoint: **{endpoint}**{note}"
        )
    else:
        note = f"  {extra_note}" if extra_note else ""
        st.caption(f"Using profile **{profile}**, region **{region}**.{note}")

    return ctx
