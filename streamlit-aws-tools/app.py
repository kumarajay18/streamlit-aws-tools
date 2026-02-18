# app.py — DPES Support Dashboard (Home)

import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

import streamlit as st
from src.aws_s3 import get_manager, SUPPORTED_PROFILES, DEFAULT_REGION, DEFAULT_EXPORT_NAME

# -----------------------------------------------------------------------------
# Page Setup
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="DPES AWS Tools",
    page_icon="🧰",
    layout="wide"
)

mgr = get_manager()

# -----------------------------------------------------------------------------
# Header Section with Qantas Logo & Title
# -----------------------------------------------------------------------------
header = st.container()
with header:
    st.image("assets/qantas_logo_v.png",width="stretch")
    st.markdown(
            """
            <div style="padding-top: 12px;">
                <h1 style="margin-bottom: 0;">DPES Support Dashboard</h1>
                <h4 style="color: #444; margin-top: 0;">AWS S3 Tools & Utilities</h4>
            </div>
            """,
            unsafe_allow_html=True
        )

st.markdown("---")

# -----------------------------------------------------------------------------
# Top Navigation Bar
# -----------------------------------------------------------------------------
nav = st.container()
with nav:
    nav1, nav2, nav3, nav4 = st.columns([1, 1, 1, 1])

    with nav1:
        if st.button("🏠 Home", width='stretch'):
            st.switch_page("app.py")

    with nav2:
        if st.button("📄 List Files", width='stretch'):
            st.switch_page("pages/1_List_Files.py")

    with nav3:
        if st.button("⬇️ Download Files", width='stretch'):
            st.switch_page("pages/2_Download_Files.py")

    with nav4:
        if st.button("⬆️ Upload Files", width='stretch'):
            st.switch_page("pages/3_Upload_Files.py")

st.markdown("---")

# -----------------------------------------------------------------------------
# Sidebar: Login Controls
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("🔐 AWS Login")

    profile = st.radio(
        "AWS Profile",
        SUPPORTED_PROFILES,
        index=0,
    )

    region = st.text_input("AWS Region", value=os.getenv("AWS_DEFAULT_REGION", DEFAULT_REGION))
    export_name = st.text_input("S3 Endpoint Export Name", value=DEFAULT_EXPORT_NAME)

    run_sso = st.checkbox("Run `aws sso login` now", value=True)

    st.markdown("---")

    # Login Button
    if st.button("🔓 Login with AWS SSO", type="primary", width='stretch'):
        try:
            with st.status("Logging in to AWS SSO...", expanded=True) as status:
                ctx = mgr.login_and_setup(
                    profile=profile,
                    region=region,
                    export_name=export_name,
                    run_sso=run_sso
                )

                st.success("Login Successful")
                st.write(f"**Account**: {ctx['identity']['Account']}")
                st.write(f"**ARN**: {ctx['identity']['Arn']}")
                st.write(f"**S3 Endpoint**: {ctx['s3_endpoint_url'] or '(standard)'}")

                # Persist session
                st.session_state["aws_profile"] = profile
                st.session_state["aws_region"] = region
                st.session_state["s3_endpoint_url"] = ctx["s3_endpoint_url"]

                status.update(label="Login Complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Login failed: {e}")

    # Reuse Existing Session
    if st.button("♻️ Reuse Existing Session", width='stretch'):
        try:
            with st.status("Reusing existing AWS session...", expanded=True) as status:
                ctx = mgr.login_and_setup(
                    profile=profile,
                    region=region,
                    export_name=export_name,
                    run_sso=False
                )

                st.success("Active cached session detected.")
                st.write(f"**Account**: {ctx['identity']['Account']}")
                st.write(f"**ARN**: {ctx['identity']['Arn']}")
                st.write(f"**S3 Endpoint**: {ctx['s3_endpoint_url'] or '(standard)'}")

                st.session_state["aws_profile"] = profile
                st.session_state["aws_region"] = region
                st.session_state["s3_endpoint_url"] = ctx["s3_endpoint_url"]

                status.update(label="Session Ready", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Could not reuse SSO session: {e}")
            st.info("Click **Login with AWS SSO** instead.")

# -----------------------------------------------------------------------------
# Main Body — Current Session Overview
# -----------------------------------------------------------------------------
st.subheader("🔧 Current AWS Session")

session_box = st.container()
with session_box:
    if mgr.has_active_session():
        ctx = mgr.current_context()
        st.info("Session Ready")
    else:
        st.info("You are not logged in yet. Please authenticate using the left panel.")

st.markdown("---")

# -----------------------------------------------------------------------------
# Next Steps Section
# -----------------------------------------------------------------------------
st.subheader("📁 Tools & Navigation")

colA, colB, colC = st.columns(3)

with colA:
    if st.button("📄 Open List Files", width='stretch'):
        st.switch_page("pages/1_List_Files.py")

with colB:
    if st.button("⬇️ Open Download Files", width='stretch'):
        st.switch_page("pages/2_Download_Files.py")

with colC:
    if st.button("⬆️ Open Upload Files", width='stretch'):
        st.switch_page("pages/3_Upload_Files.py")

st.markdown("---")

# -----------------------------------------------------------------------------
# Optional Debug (Hidden by Default)
# -----------------------------------------------------------------------------
with st.expander("🛠 Debug Information (optional)"):
    st.write("CWD:", os.getcwd())
