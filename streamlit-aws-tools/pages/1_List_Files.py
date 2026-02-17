# pages/1_List_Files.py
import streamlit as st
from datetime import datetime, timedelta

from src.aws_s3 import get_manager
from src.core.common import S3Utils
from src.core.s3_browser import S3Browser

st.set_page_config(page_title="List S3 Files", page_icon="📄", layout="wide")
st.title("📄 List S3 Files")

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

# -----------------------------
# Inputs
# -----------------------------
with st.sidebar:
    st.header("S3 Query")
    s3_path = st.text_input(
        "S3 path (bucket or bucket/prefix)",
        value=st.session_state.get("s3_path", ""),
        placeholder="e.g., s3://my-bucket/folder/subfolder/ or my-bucket/folder",
        help="Enter a full S3 path. We will parse bucket & prefix for you."
    )

    versions_mode = st.checkbox(
        "List object versions",
        value=False,
        help="When checked, shows object versions. Use option below to include delete markers (deleted files)."
    )
    show_delete_markers = st.checkbox(
        "Include delete markers (deleted files)",
        value=False,
        help="Show 'delete markers' that represent deleted files in a versioned bucket."
    )

    st.markdown("### Filter by time")
    enable_time_filter = st.checkbox("Enable datetime range filter", value=False)
    tz = datetime.now().astimezone().tzinfo
    default_end = datetime.now(tz=tz).replace(microsecond=0)
    default_start = default_end - timedelta(days=1)
    col_s, col_e = st.columns(2)
    with col_s:
        start_dt = st.datetime_input("Start", value=st.session_state.get("start_dt", default_start))
    with col_e:
        end_dt = st.datetime_input("End", value=st.session_state.get("end_dt", default_end))

    st.session_state["start_dt"] = start_dt
    st.session_state["end_dt"] = end_dt

    st.markdown("---")
    max_items = st.number_input(
        "Max results to display",
        min_value=1, max_value=10000, value=1000, step=100
    )

# -----------------------------
# Actions
# -----------------------------
list_btn = st.button("List Objects", type="primary", width='stretch')
col_a, col_b = st.columns(2)
with col_a:
    latest_btn = st.button("Get Latest File", width='stretch', disabled=versions_mode)
with col_b:
    clear_filter_btn = st.button("Clear Filter", width='stretch')

if clear_filter_btn:
    st.session_state["start_dt"] = (datetime.now().astimezone().replace(microsecond=0) - timedelta(days=1))
    st.session_state["end_dt"] = datetime.now().astimezone().replace(microsecond=0)
    st.rerun()

# -----------------------------
# Handlers
# -----------------------------
if list_btn:
    try:
        bucket, prefix = S3Utils.parse_s3_path(s3_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    st.session_state["s3_path"] = s3_path
    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    with st.status(f"{'Listing object versions' if versions_mode else 'Listing objects'} from s3://{bucket}/{prefix}", expanded=True) as status:
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
            else:
                if enable_time_filter:
                    st.info(f"Filtered LastModified between **{start_utc}** and **{end_utc}** (UTC).")
                st.success(f"Showing {len(rows)} (capped at {max_items}).")
                st.dataframe(rows, width='stretch')

                if not versions_mode:
                    subs = browser.summarize_subfolders([r["Key"] for r in rows], prefix)
                    with st.expander(
                        f"📁 Subfolders under '{prefix or '/'}' (first {min(len(subs), 50)} shown)",
                        expanded=False
                    ):
                        if not subs:
                            st.write("No subfolders detected.")
                        else:
                            for i, name in enumerate(subs[:50], start=1):
                                st.write(f"{i}. {name}")

            ctx = mgr.current_context()
            st.caption(
                f"Profile: **{ctx.get('profile')}**, Region: **{ctx.get('region')}**, "
                f"S3 Endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**"
            )
            status.update(label="Listing complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Error: {e}")

if latest_btn:
    try:
        bucket, prefix = S3Utils.parse_s3_path(s3_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    with st.status(f"Finding latest object under s3://{bucket}/{prefix} ...", expanded=True) as status:
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
        status.update(label="Complete", state="complete", expanded=False)