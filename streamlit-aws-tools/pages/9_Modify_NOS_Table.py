# pages/6_Modify_NOS_Table.py

from __future__ import annotations

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

from src.aws_s3 import get_manager
from src.ui.topbar import render_topbar  # if you have a topbar, it will render here too

st.set_page_config(page_title="Modify NOS Table", page_icon="🛠️", layout="wide")

# -----------------------------
# Session / AWS
# -----------------------------
mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Use the top bar to log in.")
    st.stop()

default_region = "ap-southeast-2"
region = st.session_state.get("nos_dynamo_region", default_region)
pipeline_id = st.session_state.get("nos_pipeline_id", "")

st.title("🛠️ Modify NOS Table")

with st.sidebar:
    st.header("Context")
    pipeline_id = st.text_input("PipelineId", value=pipeline_id, help="From Lambda env PIPELINE_ID")
    st.session_state["nos_pipeline_id"] = pipeline_id

    region = st.text_input("Region", value=region)
    st.session_state["nos_dynamo_region"] = region

# -----------------------------
# Helpers
# -----------------------------
def find_nos_table_name(dynamo_client) -> Optional[str]:
    """
    Find the DynamoDB table whose name contains both 'TeradataNosIntegration' and 'NosState'.
    Returns the first match, or None if not found.
    """
    try:
        paginator = dynamo_client.get_paginator("list_tables")
        for page in paginator.paginate():
            for name in page.get("TableNames", []):
                lname = name.lower()
                if "teradatanosintegration".lower() in lname and "nosstate".lower() in lname:
                    return name
    except Exception:
        pass
    return None


def scan_items_for_pipeline(table, pipeline_id: str) -> List[Dict]:
    """
    Scan the table for items with PipelineId == pipeline_id
    """
    from boto3.dynamodb.conditions import Attr  # type: ignore
    items: List[Dict] = []
    if not pipeline_id:
        return items

    fe = Attr("PipelineId").eq(pipeline_id)
    kwargs = {"FilterExpression": fe}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" in resp:
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        else:
            break
    return items


def normalize_rows(items: List[Dict]) -> Tuple[pd.DataFrame, Dict[int, Dict]]:
    """
    Convert raw DynamoDB items to a DataFrame of the expected columns.
    Also returns a map batch_id -> key dict for updates.
    Assumes PK = PipelineId (S) and SK = BatchId (N).
    """
    rows = []
    key_map: Dict[int, Dict] = {}
    for it in items:
        pid = it.get("PipelineId")
        bid_raw = it.get("BatchId")
        # BatchId could be Decimal; convert to int if safe
        try:
            bid = int(bid_raw) if bid_raw is not None else None
        except Exception:
            # leave as-is; skip key-map if not numeric
            bid = bid_raw

        row = {
            "PipelineId": pid,
            "BatchId": bid,
            "IsCurrent": it.get("IsCurrent"),
            "IsTransformed": it.get("IsTransformed"),
            "TransformedTimestamp": it.get("TransformedTimestamp") or it.get("TransformedTimestamp", None),
        }
        rows.append(row)

        if pid is not None and isinstance(bid, int):
            key_map[bid] = {"PipelineId": pid, "BatchId": bid}

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "PipelineId", "BatchId", "IsCurrent", "IsTransformed", "TransformedTimestamp"
    ])
    # Sort by BatchId if present
    if "BatchId" in df.columns and not df.empty:
        try:
            df = df.sort_values(by=["BatchId"], ascending=False)
        except Exception:
            pass
    return df, key_map


def apply_updates(table, key_map: Dict[int, Dict], batch_ids: List[int], set_current: bool, set_transformed: bool) -> Tuple[int, List[str]]:
    """
    Update selected batches. Returns (updated_count, errors)
    """
    updated = 0
    errors: List[str] = []
    for b in batch_ids:
        key = key_map.get(b)
        if not key:
            errors.append(f"BatchId {b} not found in current result set.")
            continue
        try:
            table.update_item(
                Key={"PipelineId": key["PipelineId"], "BatchId": key["BatchId"]},
                UpdateExpression="SET IsCurrent = :c, IsTransformed = :t",
                ExpressionAttributeValues={
                    ":c": set_current,
                    ":t": set_transformed,
                },
            )
            updated += 1
        except ClientError as e:
            errors.append(f"BatchId {b}: {e.response.get('Error', {}).get('Message', str(e))}")
        except BotoCoreError as e:
            errors.append(f"BatchId {b}: {e}")
        except Exception as e:
            errors.append(f"BatchId {b}: {e}")
    return updated, errors

# -----------------------------
# Main UI logic
# -----------------------------
if not pipeline_id.strip():
    st.info("Enter a PipelineId (or return from the App Discovery page using **Modify NOS Table**).")
    st.stop()

session = mgr.get_session()
dynamo_client = session.client("dynamodb", region_name=region)
dynamo_resource = session.resource("dynamodb", region_name=region)

with st.status("Finding NOS State table ...", expanded=False) as status:
    table_name = find_nos_table_name(dynamo_client)
    if not table_name:
        st.error("Could not find a table containing both 'TeradataNosIntegration' and 'NosState'.")
        status.update(label="Failed", state="error")
        st.stop()
    else:
        status.update(label=f"Found table: **{table_name}**", state="complete")

table = dynamo_resource.Table(table_name)

# Load items
with st.status(f"Scanning items where PipelineId == **{pipeline_id}** ...", expanded=False) as status:
    items = scan_items_for_pipeline(table, pipeline_id)
    status.update(label=f"Found {len(items)} item(s)", state="complete", expanded=False)

df, key_map = normalize_rows(items)

st.markdown(f"### 📋 Items for PipelineId: `{pipeline_id}`")
st.dataframe(df, width='stretch', hide_index=True)

st.markdown("---")
st.subheader("✏️ Update Flags")

# From results: allow selecting one or more batches
available_batches = [int(x) for x in df["BatchId"].dropna().tolist()] if not df.empty else []
selected_batches = st.multiselect("Select BatchId(s) from results", options=available_batches)

# Or manual entry: comma-separated
manual_entry = st.text_input("Or enter BatchId(s) comma-separated (e.g., 101, 102)", value="")
manual_batches: List[int] = []
if manual_entry.strip():
    try:
        manual_batches = [int(x.strip()) for x in manual_entry.split(",") if x.strip()]
    except Exception:
        st.warning("Some BatchId values could not be parsed as integers; they will be ignored.")

# Effective target batches
target_batches = sorted(set(selected_batches + manual_batches))

col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    set_current = st.selectbox("IsCurrent", options=[True, False], index=0)
with col2:
    set_transformed = st.selectbox("IsTransformed", options=[True, False], index=0)
with col3:
    st.caption("Choose one or more BatchIds and set desired flags. Updates are applied with DynamoDB `UpdateItem`.")

# Apply updates
if st.button("✅ Apply Updates", type="primary", width='stretch', disabled=(len(target_batches) == 0)):
    with st.status("Applying updates ...", expanded=True) as status:
        updated, errs = apply_updates(table, key_map, target_batches, set_current, set_transformed)
        if updated:
            st.success(f"Updated {updated} item(s).")
        if errs:
            for e in errs:
                st.error(e)
        status.update(label="Done", state="complete")

    # Refresh results after updates
    with st.status("Refreshing items ...", expanded=False) as status:
        items = scan_items_for_pipeline(table, pipeline_id)
        df, key_map = normalize_rows(items)
        status.update(label="Refreshed", state="complete")
    st.dataframe(df, width='stretch', hide_index=True)