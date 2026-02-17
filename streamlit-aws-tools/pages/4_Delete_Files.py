# pages/4_Delete_Files.py
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

from src.aws_s3 import get_manager
from src.core.common import S3Utils
from src.core.s3_browser import S3Browser
from src.core.s3_deleter import S3Deleter


st.set_page_config(page_title="Delete S3 Files", page_icon="🗑️", layout="wide")
st.title("🗑️ Delete S3 Files (with Preview)")

# -----------------------------
# Require an active session
# -----------------------------
mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Go to the home page and click **Login with AWS SSO** first (or Reuse Existing Session).")
    st.stop()

ctx = mgr.current_context()
st.caption(
    f"Using profile **{ctx.get('profile')}**, region **{ctx.get('region')}**. "
    f"S3 endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**"
)

s3 = mgr.get_s3_client()
browser = S3Browser(s3)
deleter = S3Deleter(s3)

# -----------------------------
# Inputs (Sidebar)
# -----------------------------
with st.sidebar:
    st.header("Delete from S3")

    s3_path = st.text_input(
        "S3 path (bucket or bucket/prefix)",
        value=st.session_state.get("del_s3_path", st.session_state.get("s3_path", "")),
        placeholder="e.g., s3://my-bucket/folder/ or my-bucket/folder"
    )

    versions_mode = st.checkbox(
        "Work with versions",
        value=False,
        help="When enabled, preview object versions (and optionally delete specific versions or delete markers)."
    )
    show_delete_markers = st.checkbox(
        "Include delete markers (deleted files)",
        value=True if versions_mode else False
    )

    st.markdown("### Filter by time (optional)")
    enable_time_filter = st.checkbox(
        "Enable datetime range filter",
        value=False
    )

    tz = datetime.now().astimezone().tzinfo
    default_end = datetime.now(tz=tz).replace(microsecond=0)
    default_start = (default_end - timedelta(days=1))
    col_s, col_e = st.columns(2)
    with col_s:
        start_dt = st.datetime_input("Start", value=st.session_state.get("del_start_dt", default_start))
    with col_e:
        end_dt = st.datetime_input("End", value=st.session_state.get("del_end_dt", default_end))

    st.session_state["del_start_dt"] = start_dt
    st.session_state["del_end_dt"] = end_dt

    st.markdown("---")
    max_preview = st.number_input("Max objects to preview", min_value=1, max_value=10000, value=500, step=50)

# -----------------------------
# Actions
# -----------------------------
c1, c2 = st.columns([1, 1])
with c1:
    preview_btn = st.button("🔍 Preview", type="primary", width='stretch')
with c2:
    clear_btn = st.button("🧹 Clear", width='stretch')

if clear_btn:
    for k in ["del_results_df", "del_meta"]:
        st.session_state.pop(k, None)
    st.rerun()

st.warning("⚠️ Deletions are irreversible. Use the preview and selection carefully.", icon="⚠️")

# -----------------------------
# Preview
# -----------------------------
if preview_btn:
    try:
        bucket, prefix = S3Utils.parse_s3_path(s3_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    st.session_state["del_s3_path"] = s3_path
    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    with st.status(f"Previewing under s3://{bucket}/{prefix}", expanded=True) as status:
        try:
            if versions_mode:
                rows = browser.list_object_versions(
                    bucket=bucket,
                    prefix=prefix,
                    cap=max_preview,
                    start_utc=start_utc,
                    end_utc=end_utc,
                    include_delete_markers=show_delete_markers
                )
            else:
                rows = browser.list_objects(
                    bucket=bucket,
                    prefix=prefix,
                    cap=max_preview,
                    start_utc=start_utc,
                    end_utc=end_utc
                )

            if not rows:
                st.info("No objects found for that path.")
                st.session_state.pop("del_results_df", None)
                st.session_state.pop("del_meta", None)
            else:
                if enable_time_filter:
                    st.info(f"Filtered LastModified between **{start_utc}** and **{end_utc}** (UTC).")
                st.success(f"Showing {len(rows)} (capped at {max_preview}).")

                df = pd.DataFrame(rows)
                df.insert(0, "Select", True)
                st.session_state["del_results_df"] = df
                st.session_state["del_meta"] = {
                    "bucket": bucket,
                    "prefix": prefix,
                    "versions_mode": versions_mode,
                    "preview_time": datetime.now().isoformat(timespec="seconds"),
                }
            status.update(label="Preview complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Unexpected error: {e}")

# -----------------------------
# Render + Delete
# -----------------------------
if "del_results_df" in st.session_state and "del_meta" in st.session_state:
    meta = st.session_state["del_meta"]
    st.markdown("### Objects to consider for deletion")
    st.caption(
        f"Location: **s3://{meta['bucket']}/{meta['prefix']}** | "
        f"Previewed: **{meta['preview_time']}** | "
        f"Mode: **{'Versions' if meta['versions_mode'] else 'Current objects'}**"
    )

    edited = st.data_editor(
        st.session_state["del_results_df"],
        key="del_editor",
        width='stretch',
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=True)
        }
    )
    st.session_state["del_results_df"] = edited

    selected = edited[edited["Select"] == True]
    st.write(f"Selected: **{len(selected)}** item(s) for deletion.")

    st.markdown("#### Confirm deletion")
    st.write("Type **DELETE** to confirm. This cannot be undone.")
    confirm_text = st.text_input("Confirmation", value="", placeholder="Type DELETE to enable the button")

    can_delete = (confirm_text.strip().upper() == "DELETE") and (len(selected) > 0)

    if st.button("🗑️ Delete Selected", type="primary", width='stretch', disabled=not can_delete):
        bucket = meta["bucket"]

        with st.status(f"Deleting {len(selected)} item(s)...", expanded=True) as status:
            if meta["versions_mode"]:
                # Delete specific versions / delete markers
                items = []
                for _, r in selected.iterrows():
                    if pd.isna(r.get("VersionId")):
                        # A row without VersionId cannot be deleted as a 'version'—ignore safely
                        continue
                    items.append({"Key": r["Key"], "VersionId": r["VersionId"]})
                deleted_count, errors = deleter.delete_versions(bucket=bucket, items=items, batch_size=1000)
            else:
                # Delete current objects (creates a delete marker if versioning is enabled)
                keys = selected["Key"].tolist()
                deleted_count, errors = deleter.delete_current(bucket=bucket, keys=keys, batch_size=1000)

            st.success(f"Deleted {deleted_count} item(s).")
            if errors:
                st.error(f"{len(errors)} error(s) occurred:")
                for e in errors[:20]:
                    if "VersionId" in e:
                        st.write(f"- {e.get('Key')} (v={e.get('VersionId')}) → {e.get('Code')}: {e.get('Message')}")
                    else:
                        st.write(f"- {e.get('Key')} → {e.get('Code')}: {e.get('Message')}")
                if len(errors) > 20:
                    st.caption(f"...and {len(errors) - 20} more")

            status.update(label="Deletion complete", state="complete", expanded=False)