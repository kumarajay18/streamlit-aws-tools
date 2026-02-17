# pages/7_ETL_QA_Tools.py
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

from src.aws_s3 import get_manager
from src.core.common import S3Utils
from src.core.s3_browser import S3Browser
from src.core.qa_inspector import QAInspector

st.set_page_config(page_title="ETL QA Tools", page_icon="🧪", layout="wide")
st.title("🧪 ETL QA Tools")

# -----------------------------
# Require active session
# -----------------------------
mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Go to Home and **Login with AWS SSO** (or Reuse Existing).")
    st.stop()

ctx = mgr.current_context()
st.caption(
    f"Using profile **{ctx.get('profile')}**, region **{ctx.get('region')}**. "
    f"S3 endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**"
)

s3 = mgr.get_s3_client()
browser = S3Browser(s3)
qa = QAInspector(boto3_client=s3, boto3_session=mgr.get_session(), s3_endpoint_url=ctx.get("s3_endpoint_url"))

# -----------------------------
# Inputs
# -----------------------------
with st.sidebar:
    st.header("S3 Dataset / File")
    s3_path = st.text_input(
        "S3 path (file OR folder)",
        value=st.session_state.get("qa_s3_path", st.session_state.get("s3_path", "")),
        placeholder="e.g., s3://bucket/folder/ or s3://bucket/file.parquet"
    )
    st.session_state["qa_s3_path"] = s3_path

    versions_mode = st.checkbox(
        "Use versions view",
        value=False,
        help="When enabled, lists object versions (optionally including delete markers)."
    )
    show_delete_markers = st.checkbox("Include delete markers (deleted files)", value=False)

    max_list = st.number_input("Max files to scan", min_value=1, max_value=5000, value=500, step=50)

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
with c1:
    scan_btn = st.button("🔍 Scan Path", type="primary")
with c2:
    preview_btn = st.button("👀 Top 10 Rows")
with c3:
    cols_btn = st.button("🧱 List Columns")
with c4:
    count_btn = st.button("🔢 Row Count")

c5, c6 = st.columns([1, 1])
with c5:
    clear_btn = st.button("🧹 Clear")

if clear_btn:
    for k in ["qa_scan_df", "qa_selected_df"]:
        st.session_state.pop(k, None)
    st.rerun()

# -----------------------------
# Scan Path → Detect types → Build selection table
# -----------------------------
if scan_btn:
    try:
        bucket, prefix = S3Utils.parse_s3_path(s3_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    st.session_state["qa_s3_path"] = s3_path

    # Decide: file or folder (current view only)
    is_file = False
    if prefix and not prefix.endswith("/"):
        # HEAD object (current). For versions mode, we'll still treat as 'single key'.
        try:
            s3.head_object(Bucket=bucket, Key=prefix)
            is_file = True
        except Exception:
            is_file = False

    with st.status(f"Scanning s3://{bucket}/{prefix}", expanded=True) as status:
        try:
            rows = []
            if is_file and not versions_mode:
                # single current object
                obj = s3.head_object(Bucket=bucket, Key=prefix)
                key = prefix
                lm = obj["LastModified"]
                if lm and lm.tzinfo is None:
                    lm = lm.replace(tzinfo=timezone.utc)
                ftype = QAInspector.guess_type(key)
                rows.append({
                    "Select": True,
                    "S3 URI": S3Utils.build_s3_uri(bucket, key),
                    "Key": key,
                    "Type": ftype,
                    "VersionId": None,
                    "IsDeleteMarker": False,
                    "Size (MB)": round((obj.get("ContentLength", 0) or 0) / (1024 * 1024), 3),
                    "LastModified": lm,
                })
            else:
                # folder or versions view
                if versions_mode:
                    items = browser.list_object_versions(
                        bucket=bucket,
                        prefix=prefix,
                        cap=max_list,
                        include_delete_markers=show_delete_markers
                    )
                    for it in items:
                        key = it["Key"]
                        ftype = QAInspector.guess_type(key)
                        lm = it.get("LastModified")
                        rows.append({
                            "Select": not it.get("IsDeleteMarker", False),  # default skip DM for reading
                            "S3 URI": it["S3 URI"],
                            "Key": key,
                            "Type": ftype,
                            "VersionId": it.get("VersionId"),
                            "IsDeleteMarker": it.get("IsDeleteMarker", False),
                            "Size (MB)": it.get("Size (MB)"),
                            "LastModified": lm,
                        })
                else:
                    items = browser.list_objects(bucket=bucket, prefix=prefix, cap=max_list)
                    for it in items:
                        key = it["Key"]
                        ftype = QAInspector.guess_type(key)
                        lm = it.get("LastModified")
                        rows.append({
                            "Select": True,
                            "S3 URI": it["S3 URI"],
                            "Key": key,
                            "Type": ftype,
                            "VersionId": None,
                            "IsDeleteMarker": False,
                            "Size (MB)": it.get("Size (MB)"),
                            "LastModified": lm,
                        })

            if not rows:
                st.info("No files found.")
                st.session_state.pop("qa_scan_df", None)
            else:
                df = pd.DataFrame(rows)
                st.session_state["qa_scan_df"] = df
                st.success(f"Scanned {len(df)} item(s).")
                status.update(label="Scan complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Unexpected error: {e}")

# -----------------------------
# Selection table
# -----------------------------
if "qa_scan_df" in st.session_state:
    st.markdown("### Files")
    edited = st.data_editor(
        st.session_state["qa_scan_df"],
        key="qa_editor",
        hide_index=True,
        width='stretch',
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=True)
        }
    )
    st.session_state["qa_scan_df"] = edited
    selected = edited[edited["Select"] == True].copy()
    st.session_state["qa_selected_df"] = selected
    st.write(f"Selected **{len(selected)}** item(s).")

# -----------------------------
# Preview head(10) (single)
# -----------------------------
def _first_selected_row():
    if "qa_selected_df" not in st.session_state or len(st.session_state["qa_selected_df"]) == 0:
        st.error("Select at least one file.")
        return None
    if len(st.session_state["qa_selected_df"]) > 1:
        st.info("Previewing the first selected file only.")
    return st.session_state["qa_selected_df"].iloc[0]

if preview_btn:
    row = _first_selected_row()
    if row is not None:
        path = row["S3 URI"]
        key = row["Key"]
        ftype = row["Type"]
        version_id = row.get("VersionId")
        # Skip delete markers for preview
        if row.get("IsDeleteMarker", False):
            st.warning("The selected version is a delete marker (deleted file). Pick a different version.")
        else:
            try:
                bucket, _ = S3Utils.parse_s3_path(path)
            except ValueError:
                # If they clicked from df path (already s3://bucket/key?versionId=...), parse bucket from original input:
                bucket, _ = S3Utils.parse_s3_path(st.session_state.get("qa_s3_path", ""))

            with st.status(f"Loading head(10) from {path} as {ftype} ...", expanded=True) as status:
                try:
                    df_head = qa.preview_head(bucket=bucket, key=key, ftype=ftype, n=10, version_id=version_id if pd.notna(version_id) else None)
                    if df_head.empty:
                        st.info("No rows to preview.")
                    else:
                        st.dataframe(df_head, width='stretch')
                    status.update(label="Preview complete", state="complete", expanded=False)
                except Exception as e:
                    st.error(f"Preview failed: {e}")

# -----------------------------
# Columns (single)
# -----------------------------
if cols_btn:
    row = _first_selected_row()
    if row is not None:
        path = row["S3 URI"]
        key = row["Key"]
        ftype = row["Type"]
        version_id = row.get("VersionId")

        if row.get("IsDeleteMarker", False):
            st.warning("The selected version is a delete marker (deleted file). Pick a different version.")
        else:
            try:
                bucket, _ = S3Utils.parse_s3_path(path)
            except ValueError:
                bucket, _ = S3Utils.parse_s3_path(st.session_state.get("qa_s3_path", ""))

            with st.status(f"Fetching columns from {path} as {ftype} ...", expanded=True) as status:
                try:
                    cols = qa.list_columns(bucket=bucket, key=key, ftype=ftype, version_id=version_id if pd.notna(version_id) else None)
                    if not cols:
                        st.info("No columns detected.")
                    else:
                        st.write(f"**Columns ({len(cols)}):**")
                        st.code(", ".join(cols), language="text")
                    status.update(label="Columns fetched", state="complete", expanded=False)
                except Exception as e:
                    st.error(f"Column detection failed: {e}")

# -----------------------------
# Row Count (multi)
# -----------------------------
if count_btn:
    if "qa_selected_df" not in st.session_state or len(st.session_state["qa_selected_df"]) == 0:
        st.error("Select at least one file.")
        st.stop()

    sel = st.session_state["qa_selected_df"]
    total = 0
    results = []

    with st.status(f"Counting rows for {len(sel)} file(s)...", expanded=True) as status:
        try:
            for _, r in sel.iterrows():
                if r.get("IsDeleteMarker", False):
                    results.append({"S3 URI": r["S3 URI"], "Type": r["Type"], "RowCount": None, "Error": "Delete marker"})
                    continue
                key = r["Key"]
                ftype = r["Type"]
                version_id = r.get("VersionId")
                try:
                    # Parse bucket from stored s3 path or from original input
                    try:
                        bucket, _ = S3Utils.parse_s3_path(r["S3 URI"])
                    except ValueError:
                        bucket, _ = S3Utils.parse_s3_path(st.session_state.get("qa_s3_path", ""))
                    cnt = qa.rowcount(bucket=bucket, key=key, ftype=ftype, version_id=version_id if pd.notna(version_id) else None)
                    total += (cnt or 0)
                    results.append({"S3 URI": r["S3 URI"], "Type": ftype, "RowCount": cnt})
                except Exception as e:
                    results.append({"S3 URI": r["S3 URI"], "Type": ftype, "RowCount": None, "Error": str(e)})

            dfc = pd.DataFrame(results)
            st.dataframe(dfc, width='stretch')
            st.success(f"Grand Total Rows: **{total:,}**")
            status.update(label="Counting complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Counting failed: {e}")

# -----------------------------
# Notes
# -----------------------------
with st.expander("ℹ️ Notes & assumptions", expanded=False):
    st.markdown(
        """
- **Versions mode** allows scanning historical versions and **delete markers** (deleted files).
- **Preview/Columns/Row count** skip delete markers automatically since there is no readable content.
- **Parquet row count** reads Parquet metadata into memory for the selected version; this can be heavy for very large files.
- For extremely large files, prefer sampling with a smaller selection or operate outside versions mode to leverage S3 range/pushdown (where available).
        """
    )