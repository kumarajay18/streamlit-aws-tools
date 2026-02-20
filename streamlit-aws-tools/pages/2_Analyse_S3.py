# pages/2_Analyse_S3.py
import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError, BotoCoreError

from src.aws_s3 import get_manager
from src.core.common import S3Utils
from src.core.s3_browser import S3Browser
from src.core.s3_downloader import S3Downloader
from src.core.s3_deleter import S3Deleter

st.set_page_config(page_title="Analyse S3", page_icon="📦", layout="wide")
st.title("📦 Analyse S3")
REGION = "ap-southeast-2"

# ──────────────────────────────────────────────────────────────────────────────
# Require active session
# ──────────────────────────────────────────────────────────────────────────────
mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Go to the home page and log in first.")
    st.stop()

ctx = mgr.current_context()
st.caption(
    f"Using profile **{ctx.get('profile')}**, region **{REGION}**. "
    f"S3 endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**"
)

s3 = mgr.get_s3_client()
browser = S3Browser(s3)
downloader = S3Downloader(s3)
deleter = S3Deleter(s3)

# ──────────────────────────────────────────────────────────────────────────────
# Selected buckets from Discovery
# ──────────────────────────────────────────────────────────────────────────────
selected_buckets = st.session_state.get("ad_selected_buckets", [])
if not selected_buckets:
    st.info("No buckets selected yet. Go to **App Discovery** and select buckets.")
    st.stop()

st.write("**Selected buckets:** ", ", ".join(selected_buckets))

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar filters
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("List Objects Settings")

    prefix_all = st.text_input(
        "Prefix to apply to ALL buckets (optional)",
        value=st.session_state.get("s3_anal_prefix", ""),
        placeholder="e.g., folder/subfolder/"
    )
    st.session_state["s3_anal_prefix"] = prefix_all

    versions_mode = st.checkbox(
        "List object versions",
        value=st.session_state.get("s3_anal_versions", False),
        help="When checked, shows object versions. Use option below to include delete markers."
    )
    st.session_state["s3_anal_versions"] = versions_mode

    show_delete_markers = st.checkbox(
        "Include delete markers (when listing versions)",
        value=st.session_state.get("s3_anal_delmarkers", False),
    )
    st.session_state["s3_anal_delmarkers"] = show_delete_markers

    st.markdown("### Filter by time")
    enable_time_filter = st.checkbox("Enable datetime range filter", value=st.session_state.get("s3_anal_time", False))
    tz = datetime.now().astimezone().tzinfo
    default_end = datetime.now(tz=tz).replace(microsecond=0)
    default_start = default_end - timedelta(days=1)
    col_s, col_e = st.columns(2)
    with col_s:
        start_dt = st.datetime_input("Start", value=st.session_state.get("s3_anal_start_dt", default_start))
    with col_e:
        end_dt = st.datetime_input("End", value=st.session_state.get("s3_anal_end_dt", default_end))
    st.session_state["s3_anal_start_dt"] = start_dt
    st.session_state["s3_anal_end_dt"] = end_dt

    st.markdown("---")
    max_items = st.number_input(
        "Max results per bucket",
        min_value=1, max_value=10000, value=1000, step=100
    )

    st.markdown("---")
    dest_default = st.session_state.get("download_dir", str((Path.cwd() / "downloads").resolve()))
    dest_dir_str = st.text_input("Download destination (local path)", value=dest_default)
    st.session_state["download_dir"] = dest_dir_str
    preserve_structure = st.checkbox("Preserve folder structure", value=True)

# ──────────────────────────────────────────────────────────────────────────────
# Actions
# ──────────────────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    list_btn = st.button("List Objects", type="primary", use_container_width=True)
with col2:
    latest_btn = st.button("Get Latest per Bucket", use_container_width=True, disabled=versions_mode)
with col3:
    clear_btn = st.button("Clear Results", use_container_width=True)

if clear_btn:
    for k in list(st.session_state.keys()):
        if k.startswith("s3_anal_tables_") or k in ["s3_anal_results"]:
            st.session_state.pop(k, None)
    st.success("Cleared listed results (filters retained).")
    st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# List objects/versions per bucket
# ──────────────────────────────────────────────────────────────────────────────
if list_btn:
    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    results = {}
    with st.status("Listing objects across selected buckets...", expanded=True) as status:
        for bucket in selected_buckets:
            prefix = prefix_all or ""
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
                df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["Key"])
                if not df.empty:
                    df.insert(0, "Select", False)
                results[bucket] = df
                st.write(f"✔️ {bucket}: {len(df)} rows")
            except Exception as e:
                st.write(f"❌ {bucket}: {e}")

        st.session_state["s3_anal_results"] = results
        status.update(label="Listing complete", state="complete", expanded=False)

# ──────────────────────────────────────────────────────────────────────────────
# Render one table per bucket + per-table actions
# ──────────────────────────────────────────────────────────────────────────────
if "s3_anal_results" in st.session_state:
    results = st.session_state["s3_anal_results"]

    if not results:
        st.info("No results to display.")
    else:
        if enable_time_filter:
            st.info(f"Filtered LastModified between **{S3Utils.to_utc(start_dt)}** and **{S3Utils.to_utc(end_dt)}** (UTC).")

        for bucket, df in results.items():
            st.markdown(f"### 🪣 {bucket}")
            if df.empty:
                st.write("_No objects found for current filters._")
                continue

            editor_key = f"s3_anal_tables_{bucket}"
            edited = st.data_editor(
                df,
                key=editor_key,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", default=False)
                }
            )
            # persist edited table
            st.session_state["s3_anal_results"][bucket] = edited

            selected_df = edited[edited["Select"] == True]
            st.write(f"Selected: **{len(selected_df)}** item(s).")

            dest_dir = Path(os.path.expanduser(st.session_state.get("download_dir", ""))).resolve()
            st.caption(f"Download destination: `{dest_dir}`")

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button(f"⬇️ Download Selected ({bucket})", type="primary", use_container_width=True, disabled=len(selected_df) == 0):
                    items = []
                    for _, row in selected_df.iterrows():
                        it = {"Key": row["Key"]}
                        if "VersionId" in row and pd.notna(row["VersionId"]):
                            it["VersionId"] = row["VersionId"]
                        items.append(it)
                    with st.status(f"Downloading {len(items)} file(s) from {bucket} ...", expanded=True) as status:
                        saved, failed = downloader.download_many(
                            bucket=bucket,
                            items=items,
                            dest_dir=dest_dir,
                            base_prefix=prefix_all or "",
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

            with col_b:
                if st.button(f"🗑️ Delete Selected ({bucket})", type="secondary", use_container_width=True, disabled=len(selected_df) == 0):
                    # confirmation per bucket
                    conf = st.text_input(
                        f"Type DELETE to confirm deletion for bucket {bucket}",
                        key=f"confirm_del_{bucket}",
                        value="",
                        placeholder="DELETE"
                    )
                    if conf.strip().upper() != "DELETE":
                        st.warning("Please type DELETE to confirm, then press the button again.")
                    else:
                        with st.status(f"Deleting {len(selected_df)} item(s) from {bucket} ...", expanded=True) as status:
                            if versions_mode:
                                items = []
                                for _, r in selected_df.iterrows():
                                    if pd.isna(r.get("VersionId")):
                                        continue
                                    items.append({"Key": r["Key"], "VersionId": r["VersionId"]})
                                deleted_count, errors = deleter.delete_versions(bucket=bucket, items=items, batch_size=1000)
                            else:
                                keys = selected_df["Key"].tolist()
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

# ──────────────────────────────────────────────────────────────────────────────
# Latest per bucket (current objects only)
# ──────────────────────────────────────────────────────────────────────────────
if latest_btn:
    if versions_mode:
        st.warning("Latest lookup disabled while in 'versions' mode.")
    else:
        start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
        end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

        with st.status("Finding latest object per bucket...", expanded=True) as status:
            for bucket in selected_buckets:
                latest = browser.find_latest_object(bucket, prefix_all or "", start_utc=start_utc, end_utc=end_utc)
                if not latest:
                    st.write(f"- {bucket}: No objects found.")
                else:
                    st.write(f"- {bucket}: Latest object")
                    st.json({
                        "S3 URI": latest["S3 URI"],
                        "Key": latest["Key"],
                        "Size (MB)": latest["Size (MB)"],
                        "LastModified (UTC)": latest["LastModified"].isoformat() if latest["LastModified"] else None,
                        "StorageClass": latest.get("StorageClass", "STANDARD"),
                    })
            status.update(label="Lookup complete", state="complete", expanded=False)