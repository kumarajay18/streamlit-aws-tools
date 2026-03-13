# src/ui/topbar.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Dict

import streamlit as st

from src.config import SK
from src.aws_s3 import (
    get_manager,
    SUPPORTED_PROFILES,
    DEFAULT_REGION,
    DEFAULT_EXPORT_NAME,
)

# ---------- Styling (sticky top bar) ----------
_STICKY_CSS = """
<style>

/* Hide default Streamlit header */
header[data-testid="stHeader"] { display: none; }

/* Sticky top bar */
.app-topbar {
    position: sticky;
    top: 0;
    z-index: 9999;
    background: #ffffff;
    border-bottom: 1px solid #dcdcdc;
    padding: 0.2rem 0rem 0.5rem 0rem;
    box-shadow: 0px 2px 4px rgba(0,0,0,0.04);
}

/* Align logo + title cleanly */
.app-header {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 4px 12px 2px 8px;
}

/* Navigation bar container */
.nav-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 4px 8px 0px 8px;
}

/* Navigation buttons in a single line */
.nav-buttons {
    display: flex;
    gap: 6px;
}

.nav-buttons .stButton > button {
    background: #f5f5f5;
    color: #333;
    border: 1px solid #d0d0d0;
    padding: 6px 14px;
    border-radius: 6px;
    font-size: 0.85rem;
    font-weight: 500;
    transition: 0.2s ease;
}

.nav-buttons .stButton > button:hover {
    background: #ececec;
    border-color: #bcbcbc;
}

/* Session controls section */
.session-controls {
    display: flex;
    gap: 8px;
    align-items: center;
}

/* Harmonized input height */
.session-controls .stTextInput > div > div > input,
.session-controls .stSelectbox > div > div {
    height: 34px !important;
    padding-top: 4px;
    padding-bottom: 4px;
}

/* Login + Reuse buttons */
.session-controls .stButton > button {
    padding: 5px 14px;
    font-size: 0.85rem;
    border-radius: 6px;
}

/* Pills / Badges */
.badge {
    display: inline-block;
    padding: 4px 10px;
    font-size: 0.75rem;
    border-radius: 20px;
    background: #eef2f5;
    color: #334155;
    border: 1px solid #d5dbe1;
    margin-right: 6px;
}

/* Prevent content from hiding behind sticky top bar */
.block-container {
    padding-top: 1rem;
}
</style>
"""

def _logo_component():
    """Render Qantas logo if found."""
    col1, col2 = st.columns([0.15, 0.85])
    with col1:
        logo_path = Path("assets/qantas_logo.png")
        if logo_path.exists():
            try:
                st.logo(str(logo_path))
            except Exception:
                st.image(str(logo_path), width='stretch')
        else:
            st.write(" ")
    with col2:
        st.markdown(
            """
            <div style="padding-top: 10px;">
                <h2 style="margin: 0 0 2px 0;">DPES Support Dashboard</h2>
                <div style="color: #5f6c7b;">AWS S3 Tools & Utilities</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

def _nav_component():
    """Top navigation buttons (works even if page_link is not available)."""
    cols = st.columns([1, 1, 1, 1, 1, 1])
    def _btn(label, page_path):
        try:
            # Streamlit >= 1.31
            st.page_link(page_path, label=label, width='stretch')
        except Exception:
            # Fallback to switch_page
            if st.button(label, width='stretch'):
                st.switch_page(page_path)

    with cols[0]:
        _btn("🏠 Home", "app.py")  # keep if you still want a landing page; otherwise can remove
    with cols[1]:
        _btn("📄 List", "pages/1_List_Files.py")
    with cols[2]:
        _btn("⬇️ Download", "pages/2_Download_Files.py")
    with cols[3]:
        _btn("⬆️ Upload", "pages/3_Upload_Files.py")
    with cols[4]:
        _btn("🗑️ Delete", "pages/4_Delete_Files.py")
    with cols[5]:
        _btn("🧪 ETL QA", "pages/7_ETL_QA_Tools.py")

def _session_controls(mgr) -> Dict:
    """Right side: login/profile/region controls + session summary."""
    ctx = mgr.current_context() if mgr.has_active_session() else {}
    current_profile = ctx.get("profile") if ctx else st.session_state.get(SK.AWS_PROFILE)
    current_region = ctx.get("region") if ctx else st.session_state.get(SK.AWS_REGION, DEFAULT_REGION)
    current_endpoint = ctx.get("s3_endpoint_url") if ctx else st.session_state.get(SK.S3_ENDPOINT_URL, "")

    # Controls in two rows
    # Row 1: Profile/Region + Basic actions
    c1, c2, c3, c4 = st.columns([1.1, 0.9, 0.9, 1.1])

    with c1:
        profile = st.selectbox("Profile", SUPPORTED_PROFILES, index=SUPPORTED_PROFILES.index(current_profile) if current_profile in SUPPORTED_PROFILES else 0)
    with c2:
        region = st.text_input("Region", value=current_region or DEFAULT_REGION)
    with c3:
        # Reuse session (no new browser), refresh creds if cached
        if st.button("♻️ Reuse", width='stretch'):
            try:
                res = mgr.login_and_setup(profile=profile, region=region, export_name=DEFAULT_EXPORT_NAME, run_sso=False)
                st.session_state[SK.AWS_PROFILE] = profile
                st.session_state[SK.AWS_REGION] = region
                st.session_state[SK.S3_ENDPOINT_URL] = res.get("s3_endpoint_url")
                st.success("Reused existing session.")
            except Exception as e:
                st.warning(f"Reuse failed: {e}")
    with c4:
        # Full login (opens browser)
        if st.button("🔓 Login", type="primary", width='stretch'):
            try:
                res = mgr.login_and_setup(profile=profile, region=region, export_name=DEFAULT_EXPORT_NAME, run_sso=True)
                st.session_state[SK.AWS_PROFILE] = profile
                st.session_state[SK.AWS_REGION] = region
                st.session_state[SK.S3_ENDPOINT_URL] = res.get("s3_endpoint_url")
                st.success("Login successful.")
            except Exception as e:
                st.error(f"Login failed: {e}")

    # Row 2: Advanced endpoint override + force re-login + session pills
    d1, d2, d3 = st.columns([1.2, 0.8, 2.0])
    with d1:
        with st.expander("Advanced (Endpoint)", expanded=False):
            override = st.text_input("S3 Endpoint override (optional)", value=st.session_state.get(SK.S3_ENDPOINT_OVERRIDE, "")).strip()
            if st.button("Apply Override", width='stretch'):
                try:
                    # Store override and re-init without forcing browser
                    st.session_state[SK.S3_ENDPOINT_OVERRIDE] = override
                    res = mgr.login_and_setup(
                        profile=st.session_state.get("aws_profile", profile),
                        region=st.session_state.get(SK.AWS_REGION, region),
                        export_name=DEFAULT_EXPORT_NAME,
                        run_sso=False,
                        s3_endpoint_url_override=(override or None),
                    )
                    st.session_state[SK.S3_ENDPOINT_URL] = res.get("s3_endpoint_url")
                    st.success("Endpoint applied.")
                except Exception as e:
                    st.error(f"Failed to apply endpoint: {e}")
    with d2:
        if st.button("🔁 Re-login (force)", width='stretch'):
            try:
                res = mgr.login_and_setup(
                    profile=st.session_state.get("aws_profile", profile),
                    region=st.session_state.get(SK.AWS_REGION, region),
                    export_name=DEFAULT_EXPORT_NAME,
                    run_sso=True,
                    s3_endpoint_url_override=st.session_state.get(SK.S3_ENDPOINT_OVERRIDE, None) or None,
                )
                st.session_state[SK.S3_ENDPOINT_URL] = res.get("s3_endpoint_url")
                st.success("Re-login successful.")
            except Exception as e:
                st.error(f"Re-login failed: {e}")
    with d3:
        # Session summary pills
        if mgr.has_active_session():
            c = mgr.current_context()
            idn = c.get("identity") or {}
            st.markdown(
                f"""
                <div>
                    <span class="badge">Profile: {c.get('profile')}</span>
                    <span class="badge">Region: {c.get('region')}</span>
                    <span class="badge">Account: {idn.get('Account','?')}</span>
                    <span class="badge">Endpoint: {c.get('s3_endpoint_url') or 'standard'}</span>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.info("Not logged in.")

    # Return latest known context (after any click above)
    return mgr.current_context() if mgr.has_active_session() else {}

def render_topbar() -> Dict:
    """
    Draw a sticky global top bar (logo + nav + session controls).
    Returns the current context dict if logged in, else {}.
    """
    st.markdown(_STICKY_CSS, unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="app-topbar">', unsafe_allow_html=True)

        # Row 1: Logo/Title + Nav
        r1c1, r1c2 = st.columns([1.2, 1.8])
        with r1c1:
            _logo_component()
        with r1c2:
            _nav_component()

        # Row 2: Session Controls
        ctx = _session_controls(get_manager())

        st.markdown('</div>', unsafe_allow_html=True)

    return ctx