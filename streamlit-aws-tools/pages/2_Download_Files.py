# pages/2_Download_Files.py
import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from src.aws_s3 import get_manager
from src.core.common import S3Utils
from src.core.s3_browser import S3Browser
from src.core.s3_downloader import S3Downloader

st.set_page_config(page_title="Download S3 Files", page_icon="⬇️", layout="wide")
st.title("⬇️ Download S3 Files")

# -----------------------------
# Require an active session
# -----------------------------
mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Go to the home page and click **Login with AWS SSO** first.")
    st.stop()

ctx = mgr.current_context()
st.caption(
    f"Using profile **{ctx.get('profile')}**, region **{ctx.get('region')}**. "
    f"S3 endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**"
)

s3 = mgr.get_s3_client()
browser = S3Browser(s3)
downloader = S3Downloader(s3)

# -----------------------------
# Inputs (Sidebar)
# -----------------------------
with st.sidebar:
    st.header("S3 Query")
    s3_path = st.text_input(
        "S3 path (bucket or bucket/prefix)",
        value=st.session_state.get("dl_s3_path", st.session_state.get("s3_path", "")),
        placeholder="e.g., s3://my-bucket/folder or bucket/prefix"
    )

    versions_mode = st.checkbox(
        "List object versions",
        value=False,
        help="When checked, we search versions instead of current objects."
    )
    show_delete_markers = st.checkbox(
        "Include delete markers (deleted files)",
        value=False,
        help="Show delete markers in versions list."
    )

    st.markdown("### Filter by time")
    enable_time_filter = st.checkbox("Enable datetime range filter", value=False)

    tz = datetime.now().astimezone().tzinfo
    default_end = datetime.now(tz=tz).replace(microsecond=0)
    default_start = (default_end - timedelta(days=1))

    col_s, col_e = st.columns(2)
    with col_s:
        start_dt = st.datetime_input("Start", value=st.session_state.get("dl_start_dt", default_start))
    with col_e:
        end_dt = st.datetime_input("End", value=st.session_state.get("dl_end_dt", default_end))

    st.session_state["dl_start_dt"] = start_dt
    st.session_state["dl_end_dt"] = end_dt

    st.markdown("---")
    max_items = st.number_input("Max results to display", min_value=1, max_value=10000, value=1000, step=100)

    st.markdown("---")
    st.header("Download options")
    dest_default = st.session_state.get("download_dir", str((Path.cwd() / "downloads").resolve()))
    dest_dir_str = st.text_input("Destination folder (local path)", value=dest_default)
    preserve_structure = st.checkbox(
        "Preserve folder structure (relative to the prefix)",
        value=True
    )
    st.session_state["download_dir"] = dest_dir_str

# -----------------------------
# Actions
# -----------------------------
search_btn = st.button("Search", type="primary", width='stretch')
col_a, col_b = st.columns(2)
with col_a:
    latest_btn = st.button("Get Latest File", width='stretch', disabled=versions_mode)
with col_b:
    clear_filter_btn = st.button("Clear Filter", width='stretch')

if clear_filter_btn:
    st.session_state["dl_start_dt"] = (datetime.now().astimezone().replace(microsecond=0) - timedelta(days=1))
    st.session_state["dl_end_dt"] = datetime.now().astimezone().replace(microsecond=0)
    st.rerun()

# -----------------------------
# Search
# -----------------------------
if search_btn:
    try:
        bucket, prefix = S3Utils.parse_s3_path(s3_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    st.session_state["dl_s3_path"] = s3_path
    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    with st.status(f"Searching under s3://{bucket}/{prefix}", expanded=True) as status:
        try:
            if versions_mode:
                rows = browser.list_object_versions(
                    bucket=bucket,
                    prefix=prefix,
                    cap=max_items,
                    start_utc=start_utc,
                    end_utc=end_utc,
                    include_delete_markers=show_delete_markers
                )
            else:
                rows = browser.list_objects(
                    bucket=bucket,
                    prefix=prefix,
                    cap=max_items,
                    start_utc=start_utc,
                    end_utc=end_utc
                )

            if not rows:
                st.info("No results.")
                st.session_state.pop("dl_results_df", None)
            else:
                if enable_time_filter:
                    st.info(f"Filtered LastModified between **{start_utc}** and **{end_utc}** (UTC).")

                st.success(f"Found {len(rows)} (capped at {max_items}).")
                df = pd.DataFrame(rows)
                # Add Select column
                df.insert(0, "Select", False)
                st.session_state["dl_results_df"] = df

            status.update(label="Search complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Unexpected error: {e}")

# -----------------------------
# Render table + download
# -----------------------------
if "dl_results_df" in st.session_state:
    st.markdown("### Results")
    edited = st.data_editor(
        st.session_state["dl_results_df"],
        key="dl_editor",
        width='stretch',
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", help="Tick to download", default=False)
        }
    )
    st.session_state["dl_results_df"] = edited

    selected_df = edited[edited["Select"] == True]
    st.write(f"Selected: **{len(selected_df)}** file(s).")

    dest_dir = Path(os.path.expanduser(dest_dir_str)).resolve()
    st.write(f"Destination: `{dest_dir}`")

    if st.button("Download Selected", type="primary", width='stretch', disabled=len(selected_df) == 0):
        try:
            bucket, prefix = S3Utils.parse_s3_path(st.session_state.get("dl_s3_path", s3_path))
        except Exception:
            st.error("Invalid or missing S3 path. Please search again.")
            st.stop()

        items = []
        for _, row in selected_df.iterrows():
            it = {"Key": row["Key"]}
            if "VersionId" in row and pd.notna(row["VersionId"]):
                it["VersionId"] = row["VersionId"]
            items.append(it)

        with st.status(f"Downloading {len(items)} file(s)...", expanded=True) as status:
            saved, failed = downloader.download_many(
                bucket=bucket,
                items=items,
                dest_dir=dest_dir,
                base_prefix=prefix,
                preserve_structure=preserve_structure
            )

            if saved:
                st.success(f"Saved {len(saved)} file(s):")
                for p in saved[:50]:
                    st.write(f"- `{p}`")
                if len(saved) > 50:
                    st.caption(f"...and {len(saved) - 50} more")
            if failed:
                st.error(f"{len(failed)} file(s) failed:")
                for key, err in failed[:20]:
                    st.write(f"- {key} → {err}")
                if len(failed) > 20:
                    st.caption(f"...and {len(failed) - 20} more")

            status.update(label="Download complete", state="complete", expanded=False)

# -----------------------------
# Latest (current objects only)
# -----------------------------
if latest_btn:
    try:
        bucket, prefix = S3Utils.parse_s3_path(s3_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    with st.status(f"Finding latest file under s3://{bucket}/{prefix}...", expanded=True) as status:
        latest = browser.find_latest_object(bucket, prefix, start_utc, end_utc)
        if not latest:
            st.warning("No objects found.")
        else:
            st.success("Latest object:")
            st.json({
                "S3 URI": latest["S3 URI"],
                "Key": latest["Key"],
                "Size (MB)": latest["Size (MB)"],
                "LastModified (UTC)": latest["LastModified"].isoformat() if latest["LastModified"] else None,
                "StorageClass": latest.get("StorageClass", "STANDARD"),
            })
        status.update(label="Lookup complete", state="complete", expanded=False)