# pages/3_Upload_Files.py
import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
from boto3.s3.transfer import TransferConfig

from src.aws_s3 import get_manager
from src.config import SK
from src.core.common import S3Utils
from src.core.s3_uploader import S3Uploader
from src.ui.guards import require_aws_session
from src.ui.context import show_session_caption

st.set_page_config(page_title="Upload S3 Files", page_icon="⬆️", layout="wide")
st.title("⬆️ Upload Files & Folders to S3")

# -----------------------------
# Require an active session
# -----------------------------
mgr = require_aws_session()
ctx = show_session_caption()

s3 = mgr.get_s3_client()
uploader = S3Uploader(s3)

# -----------------------------
# Inputs
# -----------------------------
with st.sidebar:
    st.header("Local → S3 Upload")

    local_root_str = st.text_input(
        "Local path (file or folder)",
        value=st.session_state.get(SK.UL_LOCAL_ROOT, str((Path.cwd() / "to_upload").resolve())),
        placeholder="e.g., C:\\data\\export or /Users/you/data/export or C:\\file.csv"
    )
    st.session_state[SK.UL_LOCAL_ROOT] = local_root_str

    s3_dest_path = st.text_input(
        "Destination S3 path",
        value=st.session_state.get(SK.UL_DEST_PATH, st.session_state.get(SK.S3_PATH, "")),
        placeholder="e.g., s3://my-bucket/upload-root/  or  my-bucket/folder"
    )
    st.session_state[SK.UL_DEST_PATH] = s3_dest_path

    preserve_structure = st.checkbox(
        "Preserve folder structure (relative to the local path)",
        value=True
    )
    overwrite = st.checkbox("Overwrite if object exists", value=False)

    with st.expander("Transfer/Tuning (optional)"):
        multipart_threshold_mb = st.slider("Multipart threshold (MB)", min_value=5, max_value=128, value=8, step=1)
        max_concurrency = st.slider("Max concurrency", min_value=1, max_value=32, value=8, step=1)
        st.markdown("**Server-Side Encryption (optional)**")
        sse_opt = st.selectbox("SSE", options=["(none)", "AES256", "aws:kms"], index=0)
        kms_key_id = st.text_input("KMS Key ID (if aws:kms)", value="")

# -----------------------------
# Actions
# -----------------------------
c1, c2 = st.columns([1, 1])
with c1:
    scan_btn = st.button("🔍 Scan Local Files", type="primary", width='stretch')
with c2:
    clear_btn = st.button("🧹 Clear Results", width='stretch')

if clear_btn:
    for k in ["ul_scan_df", "ul_selected_keys"]:
        st.session_state.pop(k, None)
    st.rerun()

# -----------------------------
# Scan + Plan
# -----------------------------
if scan_btn:
    try:
        local_root = Path(os.path.expanduser(local_root_str)).resolve()
        bucket, dest_prefix = S3Utils.parse_s3_path(s3_dest_path)
    except Exception as e:
        st.error(f"Input error: {e}")
        st.stop()

    transfer_cfg = TransferConfig(
        multipart_threshold=multipart_threshold_mb * 1024 * 1024,
        max_concurrency=max_concurrency,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True,
    )

    with st.status(f"Scanning: `{local_root}`", expanded=True) as status:
        try:
            files = list(uploader.iter_local_files(local_root))
            if not files:
                st.info("No files found to upload.")
                st.session_state.pop(SK.UL_SCAN_DF, None)
                status.update(label="Scan complete", state="complete", expanded=False)
            else:
                rows = []
                total_bytes = 0
                for fp in files:
                    key = uploader.relative_key(local_root, fp, dest_prefix, preserve_structure)
                    size = fp.stat().st_size if fp.exists() else 0
                    rows.append({
                        "Select": True,
                        "Local Path": str(fp),
                        "Key": key,
                        "S3 URI": f"s3://{bucket}/{key}",
                        "Size": size,
                        "Size (Pretty)": uploader.fmt_size(size),
                        "ContentType": uploader.guess_content_type(fp) or "",
                    })
                    total_bytes += size

                df = pd.DataFrame(rows)
                st.session_state[SK.UL_SCAN_DF] = {
                    "bucket": bucket,
                    "dest_prefix": dest_prefix,
                    "local_root": str(local_root),
                    "preserve_structure": preserve_structure,
                    "scan_time": datetime.now().isoformat(timespec="seconds"),
                    "df": df,
                    "transfer_cfg": transfer_cfg,
                    "overwrite": overwrite,
                    "sse": (None if sse_opt == "(none)" else sse_opt),
                    "kms_key_id": (kms_key_id or None),
                }

                st.success(f"Found {len(df)} files. Total size ≈ {uploader.fmt_size(total_bytes)}")
                st.caption(f"Scan time: {st.session_state['ul_scan_df']['scan_time']}")
                status.update(label="Scan complete", state="complete", expanded=False)
        except Exception as e:
            st.error(f"Failed during scan: {e}")

# -----------------------------
# Upload
# -----------------------------
if SK.UL_SCAN_DF in st.session_state:
    meta = st.session_state[SK.UL_SCAN_DF]
    st.markdown("### Planned Uploads")
    st.caption(f"Destination: **s3://{meta['bucket']}/{meta['dest_prefix']}** | Preserve structure: **{meta['preserve_structure']}**")

    edited = st.data_editor(
        meta["df"],
        key="ul_editor",
        width='stretch',
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn("Select", default=True)}
    )
    st.session_state[SK.UL_SCAN_DF]["df"] = edited

    selected = edited[edited["Select"] == True]
    selected_count = len(selected)
    total_bytes_sel = int(selected["Size"].sum()) if selected_count else 0
    st.write(f"Selected: **{selected_count}** files | Total size ≈ **{uploader.fmt_size(total_bytes_sel)}**")

    if st.button("⬆️ Upload Selected", type="primary", width='stretch', disabled=(selected_count == 0)):
        bucket = meta["bucket"]
        dest_prefix = meta["dest_prefix"]
        local_root = Path(meta["local_root"])
        preserve_structure = bool(meta["preserve_structure"])
        transfer_cfg = meta["transfer_cfg"]
        overwrite = bool(meta["overwrite"])
        sse = meta.get("sse")
        kms_key_id = meta.get("kms_key_id")

        successes, skips, failures = 0, 0, 0
        prog = st.progress(0.0)
        total = len(selected)

        with st.status(f"Uploading {total} file(s)...", expanded=True) as status:
            for idx, row in selected.iterrows():
                fp = Path(row["Local Path"])
                key = row["Key"]
                ctype = row.get("ContentType") or uploader.guess_content_type(fp)

                ok, err = uploader.upload_one(
                    bucket=bucket,
                    key=key,
                    file_path=fp,
                    content_type=ctype,
                    overwrite=overwrite,
                    transfer_cfg=transfer_cfg,
                    sse=sse,
                    kms_key_id=kms_key_id
                )
                if ok:
                    successes += 1
                    st.write(f"✅ Uploaded: s3://{bucket}/{key}")
                else:
                    if err and "Skipped" in err:
                        skips += 1
                        st.write(f"⏭️ Skipped (exists): s3://{bucket}/{key}")
                    else:
                        failures += 1
                        st.write(f"❌ Failed: s3://{bucket}/{key} → {err}")

                prog.progress(min((idx + 1) / total, 1.0))

            st.markdown("---")
            st.success(f"Done. ✅ Uploaded: {successes}  ⏭️ Skipped: {skips}  ❌ Failed: {failures}")
            status.update(label="Upload complete", state="complete", expanded=False)