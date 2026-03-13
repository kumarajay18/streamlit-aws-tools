# pages/6_SQS_Send_Message.py

import json
import hashlib
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

import streamlit as st
from botocore.exceptions import ClientError

from src.aws_s3 import get_manager
from src.ui.guards import require_aws_session
from src.ui.context import show_session_caption

st.set_page_config(page_title="SQS — Send Message", page_icon="📬", layout="wide")
st.title("📬 SQS — Send Message")

try:
    c1, c2 = st.columns([0.12, 0.88])
    with c1:
        lp = Path("assets/qantas_logo.png")
        if lp.exists():
            try:
                st.logo(str(lp))
            except Exception:
                st.image(str(lp), width='stretch')
    with c2:
        st.markdown("### Qantas — Messaging")
except Exception:
    pass

# -----------------------------
# Require an active session
# -----------------------------
mgr = require_aws_session()
ctx = show_session_caption(
    extra_note="(S3 endpoint shown for reference; not used by SQS)"
)

# -----------------------------
# Helpers
# -----------------------------
def infer_fifo_from_url(queue_url: str) -> bool:
    return queue_url.strip().lower().endswith(".fifo")

def get_queue_attributes(sqs, queue_url: str) -> Dict[str, Any]:
    resp = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "All"  # includes FifoQueue, ContentBasedDeduplication, etc.
        ]
    )
    return resp.get("Attributes", {})

def safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

def gen_dedup_id(message_body_obj: Any) -> str:
    # content hash + epoch seconds for uniqueness (still deterministic per body second)
    try:
        raw = json.dumps(message_body_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except Exception:
        raw = str(message_body_obj).encode("utf-8")
    h = hashlib.sha256(raw).hexdigest()
    return f"{h}-{int(time.time())}"

def normalize_message_attributes(attrs: list[dict]) -> Dict[str, Dict[str, str]]:
    """
    Convert a list of {Name, Type, Value} rows into SQS MessageAttributes shape.
    Type: 'String' | 'Number' | 'Binary' (we'll support String & Number in UI)
    """
    out: Dict[str, Dict[str, str]] = {}
    for row in attrs:
        name = (row.get("Name") or "").strip()
        mtype = (row.get("Type") or "String").strip()
        value = (row.get("Value") or "").strip()
        if not name:
            continue
        if mtype not in ("String", "Number", "Binary"):
            mtype = "String"
        # For simplicity we treat Binary as String; true Binary would need bytes b64 handling
        out[name] = {"DataType": mtype, "StringValue": value}
    return out

# -----------------------------
# Sidebar: Queue & Options
# -----------------------------
with st.sidebar:
    st.header("Queue")
    default_queue = st.session_state.get(
        "sqs_queue_url",
        "https://sqs.ap-southeast-2.amazonaws.com/526424598388/LakeFoundations-TeradataNosIntegration-13WOD1AH9HI-NosJobQueue-sa910slzcF8I.fifo"
    )
    queue_url = st.text_input(
        "Queue URL",
        value=default_queue,
        placeholder="https://sqs.<region>.amazonaws.com/<account>/<queue-name>",
        help="Provide the full SQS Queue URL"
    )
    st.session_state["sqs_queue_url"] = queue_url

    check_btn = st.button("🔎 Check Queue & Attributes", width='stretch')

    st.markdown("---")
    st.header("Preview & Send")
    show_raw = st.checkbox("Use Raw JSON (advanced)", value=False, help="Toggle to paste/edit message body as JSON")

# -----------------------------
# Queue check (detect FIFO, dedup, etc.)
# -----------------------------
fifo_detected = infer_fifo_from_url(queue_url) if queue_url else False
content_based_dedup = None
queue_attrs = {}

if check_btn and queue_url:
    sqs = mgr.get_client("sqs")
    with st.status("Fetching queue attributes...", expanded=True) as status:
        try:
            queue_attrs = get_queue_attributes(sqs, queue_url)
            fifo_detected = queue_attrs.get("FifoQueue", "false").lower() == "true"
            content_based_dedup = queue_attrs.get("ContentBasedDeduplication", "false").lower() == "true"
            st.write("Attributes:")
            st.json(queue_attrs)
            status.update(label="Queue attributes loaded", state="complete", expanded=False)
        except ClientError as e:
            st.error(f"AWS error: {e}")
        except Exception as e:
            st.error(f"Failed to load queue attributes: {e}")

# -----------------------------
# Body inputs
# -----------------------------
st.markdown("#### Message Body")

default_body = {
    "enterprise_app_id": "a1115",
    "lake_environment": "nonprod",
    "pipeline_id": "ccdfa690-c4fb-11f0-9838-024e2965737f",
    "etl_artifact_location": "s3://deploymentfoundations-artefactsbucket-1frkdho6nzmgh/date=2026-02-03/branch=develop/run_number=2963/run_attempt=1/etl/OO",
    "batch_id": 267,
    "nos_etl_timeout": 1800,
    "enterprise_integration_appid": "a154",
}

if show_raw:
    raw_default = st.session_state.get("sqs_raw_body", json.dumps(default_body, indent=2))
    raw_json = st.text_area("Raw JSON", value=raw_default, height=220, placeholder="{ ... }")
    st.session_state["sqs_raw_body"] = raw_json
    body_form_obj: Optional[dict] = None
else:
    # Form mode
    col1, col2 = st.columns(2)
    with col1:
        enterprise_app_id = st.text_input("enterprise_app_id", value=default_body["enterprise_app_id"])
        lake_environment = st.text_input("lake_environment", value=default_body["lake_environment"])
        pipeline_id = st.text_input("pipeline_id", value=default_body["pipeline_id"])
        etl_artifact_location = st.text_input("etl_artifact_location", value=default_body["etl_artifact_location"])
    with col2:
        batch_id = st.number_input("batch_id", min_value=0, value=default_body["batch_id"], step=1)
        nos_etl_timeout = st.number_input("nos_etl_timeout", min_value=0, value=default_body["nos_etl_timeout"], step=60)
        enterprise_integration_appid = st.text_input("enterprise_integration_appid", value=default_body["enterprise_integration_appid"])

    body_form_obj = {
        "enterprise_app_id": enterprise_app_id.strip(),
        "lake_environment": lake_environment.strip(),
        "pipeline_id": pipeline_id.strip(),
        "etl_artifact_location": etl_artifact_location.strip(),
        "batch_id": int(batch_id),
        "nos_etl_timeout": int(nos_etl_timeout),
        "enterprise_integration_appid": enterprise_integration_appid.strip(),
    }

# Message attributes editor
st.markdown("##### Message Attributes (optional)")
attrs_df = st.session_state.get("sqs_msg_attrs_df")
if attrs_df is None:
    import pandas as pd
    attrs_df = pd.DataFrame([{"Name": "", "Type": "String", "Value": ""}])
attrs_editor = st.data_editor(
    attrs_df,
    key="sqs_attrs_editor",
    hide_index=True,
    width='stretch',
    column_config={
        "Name": st.column_config.TextColumn("Name"),
        "Type": st.column_config.SelectboxColumn("Type", options=["String", "Number", "Binary"], default="String"),
        "Value": st.column_config.TextColumn("Value"),
    }
)
st.session_state["sqs_msg_attrs_df"] = attrs_editor

# -----------------------------
# FIFO options (if relevant)
# -----------------------------
st.markdown("#### FIFO Options")
fifo_label = "Detected" if fifo_detected else "Not detected"
st.caption(f"FIFO: **{fifo_label}** (we infer from URL and/or attributes).")

colf1, colf2, colf3 = st.columns(3)
with colf1:
    msg_group_id = st.text_input("MessageGroupId (FIFO only)", value="ccdfa690-c4fb-11f0-9838-024e2965737f")
with colf2:
    dedup_mode = st.selectbox(
        "Deduplication",
        options=["Generate automatically", "Provide explicitly", "Content-based (queue setting)"],
        index=0,
        help="Content-based dedup requires the queue to have ContentBasedDeduplication enabled.",
    )
with colf3:
    provided_dedup = st.text_input("MessageDeduplicationId (if Provide explicitly)", value="")

if content_based_dedup is None and fifo_detected and check_btn:
    st.info("Content-based deduplication not confirmed; click 'Check Queue & Attributes' to detect. "
            "If enabled, you may leave Dedup ID blank.", icon="ℹ️")
elif content_based_dedup:
    st.success("Queue has ContentBasedDeduplication=TRUE. You may omit the Dedup ID.", icon="✅")

# -----------------------------
# Send message
# -----------------------------
st.markdown("---")
send_btn = st.button("📤 Send Message", type="primary", width='stretch')

if send_btn:
    if not queue_url.strip():
        st.error("Please provide a Queue URL.")
        st.stop()

    # Build body
    if show_raw:
        try:
            body_obj = safe_json_loads(raw_json)
        except ValueError as e:
            st.error(str(e))
            st.stop()
    else:
        body_obj = body_form_obj

    # Build MessageAttributes
    msg_attrs = normalize_message_attributes(st.session_state["sqs_msg_attrs_df"].to_dict(orient="records"))

    sqs = mgr.get_client("sqs")

    params: Dict[str, Any] = {
        "QueueUrl": queue_url.strip(),
        "MessageBody": json.dumps(body_obj),
    }
    if msg_attrs:
        params["MessageAttributes"] = msg_attrs

    # FIFO specifics
    if fifo_detected:
        if not msg_group_id.strip():
            st.error("FIFO queue requires a MessageGroupId.")
            st.stop()
        params["MessageGroupId"] = msg_group_id.strip()

        if dedup_mode == "Generate automatically":
            params["MessageDeduplicationId"] = gen_dedup_id(body_obj)
        elif dedup_mode == "Provide explicitly":
            if not provided_dedup.strip():
                st.error("Provide a MessageDeduplicationId or change dedup mode.")
                st.stop()
            params["MessageDeduplicationId"] = provided_dedup.strip()
        else:
            # Content-based dedup (leave out DedupId); warn if we didn't confirm attribute
            if content_based_dedup is False:
                st.warning("ContentBasedDeduplication appears to be disabled on this queue; AWS may reject the message.")
            # No MessageDeduplicationId added

    with st.status("Sending message…", expanded=True) as status:
        try:
            resp = sqs.send_message(**params)
            st.success("✅ Message sent successfully!")
            st.json({
                "MessageId": resp.get("MessageId"),
                "MD5OfMessageBody": resp.get("MD5OfMessageBody"),
                "MD5OfMessageAttributes": resp.get("MD5OfMessageAttributes"),
                "SequenceNumber": resp.get("SequenceNumber"),  # FIFO only
            })
            status.update(label="Sent", state="complete", expanded=False)
        except ClientError as e:
            st.error(f"AWS error: {e}")
        except Exception as e:
            st.error(f"Failed to send: {e}")