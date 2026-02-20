# pages/1_App_Discovery.py
from __future__ import annotations

import re
import json
import base64
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

from src.aws_s3 import get_manager

# ──────────────────────────────────────────────────────────────────────────────
# Page config & constants
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="App Discovery", page_icon="🔎", layout="wide")
st.title("🔎 App Discovery")
REGION = "ap-southeast-2"

# ──────────────────────────────────────────────────────────────────────────────
# Require active session
# ──────────────────────────────────────────────────────────────────────────────
mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Use the home/top bar to log in.")
    st.stop()

ctx = mgr.current_context()
st.caption(
    f"Using profile **{ctx.get('profile')}**, region **{REGION}**. "
    f"S3 endpoint: **{ctx.get('s3_endpoint_url') or 'standard'}**"
)

s3 = mgr.get_s3_client()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def parse_appids(raw: str) -> List[str]:
    """Split by commas/newlines/whitespace and return non-empty tokens."""
    if not raw:
        return []
    parts = re.split(r"[,\s]+", raw.strip())
    return [p for p in parts if p]

def needles_for_appids(appids: List[str]) -> Set[str]:
    needles: Set[str] = set()
    for a in appids:
        a = a.strip()
        if not a:
            continue
        a_low = a.lower()
        digits = "".join(ch for ch in a if ch.isdigit())
        needles.add(a_low)
        if digits:
            needles.add(digits)
    return needles

def name_matches_needles(name: str, needles: Set[str]) -> bool:
    ln = (name or "").lower()
    return any(n in ln for n in needles)

def list_buckets_matching(s3_client, appids: List[str]) -> List[Dict]:
    try:
        resp = s3_client.list_buckets()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError(
                "AccessDenied: The role is not allowed to list all buckets (s3:ListAllMyBuckets)."
            ) from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to list buckets: {e}") from e

    needles = needles_for_appids(appids)
    rows = []
    for b in resp.get("Buckets", []):
        name = b.get("Name", "")
        if name and (not needles or name_matches_needles(name, needles)):
            rows.append({"Bucket": name, "CreationDate": b.get("CreationDate")})
    return rows

def list_lambda_functions_matching(session, appids: List[str]) -> List[Dict]:
    lam = session.client("lambda", region_name=REGION)
    needles = needles_for_appids(appids)

    paginator = lam.get_paginator("list_functions")
    try:
        pages = paginator.paginate()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError(
                "AccessDenied: Not allowed to ListFunctions in this region."
            ) from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to list Lambda functions: {e}") from e

    out = []
    for page in pages:
        for fn in page.get("Functions", []):
            name = fn.get("FunctionName", "")
            if name and (not needles or name_matches_needles(name, needles)):
                out.append({
                    "FunctionName": name,
                    "Runtime": fn.get("Runtime"),
                    "LastModified": fn.get("LastModified"),
                    "Arn": fn.get("FunctionArn"),
                })
    return out

def console_link_s3_bucket(bucket: str) -> str:
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={REGION}&tab=objects"

def console_link_lambda(fn_name: str) -> str:
    return f"https://{REGION}.console.aws.amazon.com/lambda/home?region={REGION}#/functions/{fn_name}?tab=configuration"

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar - inputs & actions
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔎 Input App IDs")
    appids_text = st.text_area(
        "Enter one or more App IDs (comma/space/newline separated)",
        value=st.session_state.get("ad_appids", "A1401"),
        height=80,
        help="Examples: A1401, A1507 or multiple on separate lines"
    )
    st.session_state["ad_appids"] = appids_text

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        btn_s3 = st.button("🔍 List S3 Buckets", type="primary", use_container_width=True)
    with c2:
        btn_lambda = st.button("🔍 List Lambda Functions", use_container_width=True)

    st.markdown("---")
    st.caption("Select resources below and then choose an analysis action.")

# ──────────────────────────────────────────────────────────────────────────────
# Handlers - List S3 buckets
# ──────────────────────────────────────────────────────────────────────────────
if btn_s3:
    appids = parse_appids(st.session_state.get("ad_appids", ""))
    with st.status(f"Searching S3 buckets for **{', '.join(appids) or 'all'}**...", expanded=True) as status:
        try:
            buckets = list_buckets_matching(s3, appids)
            if not buckets:
                st.info("No buckets matched.")
                st.session_state.pop("ad_buckets_df", None)
            else:
                df = pd.DataFrame(buckets).sort_values("Bucket")
                # add Select col
                df.insert(0, "Select", False)
                st.session_state["ad_buckets_df"] = df.reset_index(drop=True)
                st.success(f"Found {len(df)} matching bucket(s).")
            status.update(label="S3 search complete", state="complete", expanded=False)
        except Exception as e:
            st.error(str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Handlers - List Lambda functions
# ──────────────────────────────────────────────────────────────────────────────
if btn_lambda:
    appids = parse_appids(st.session_state.get("ad_appids", ""))
    with st.status(f"Searching Lambda functions in **{REGION}**...", expanded=True) as status:
        try:
            functions = list_lambda_functions_matching(mgr.get_session(), appids)
            if not functions:
                st.info("No Lambda functions matched.")
                st.session_state.pop("ad_lambdas_df", None)
            else:
                df = pd.DataFrame(functions).sort_values("FunctionName")
                df.insert(0, "Select", False)
                st.session_state["ad_lambdas_df"] = df.reset_index(drop=True)
                st.success(f"Found {len(df)} matching function(s).")
            status.update(label="Lambda search complete", state="complete", expanded=False)
        except Exception as e:
            st.error(str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Render S3 buckets table (selectable) + console links
# ──────────────────────────────────────────────────────────────────────────────
if "ad_buckets_df" in st.session_state:
    st.markdown("### 🪣 Buckets")
    edited_buckets = st.data_editor(
        st.session_state["ad_buckets_df"],
        key="ad_buckets_editor",
        use_container_width=True,
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False)
        }
    )
    st.session_state["ad_buckets_df"] = edited_buckets

    with st.expander("Open in AWS Console (first 25)"):
        for b in edited_buckets["Bucket"].tolist()[:25]:
            st.markdown(f"- {console_link_s3_bucket(b)}")

# ──────────────────────────────────────────────────────────────────────────────
# Render Lambda functions table (selectable) + console links
# ──────────────────────────────────────────────────────────────────────────────
if "ad_lambdas_df" in st.session_state:
    st.markdown("### 🪄 Lambda Functions")
    edited_lambdas = st.data_editor(
        st.session_state["ad_lambdas_df"],
        key="ad_lambdas_editor",
        use_container_width=True,
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False)
        }
    )
    st.session_state["ad_lambdas_df"] = edited_lambdas

    with st.expander("Open in AWS Console (first 25)"):
        for name in edited_lambdas["FunctionName"].tolist()[:25]:
            st.markdown(f"- {console_link_lambda(name)}")

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Analysis navigation buttons (keeps discovery page state intact)
# ──────────────────────────────────────────────────────────────────────────────
col_an_s3, col_an_lambda, col_clear = st.columns([1, 1, 1])
with col_an_s3:
    if st.button("📦 Analyse S3 (Selected)", type="primary", use_container_width=True):
        sel_buckets = []
        if "ad_buckets_df" in st.session_state:
            df = st.session_state["ad_buckets_df"]
            sel_buckets = df[df["Select"] == True]["Bucket"].tolist()
        if not sel_buckets:
            st.warning("Select at least one bucket in the table above.")
        else:
            st.session_state["ad_selected_buckets"] = sel_buckets
            st.switch_page("pages/2_Analyse_S3.py")

with col_an_lambda:
    if st.button("🧠 Analyse Lambda (Selected)", type="primary", use_container_width=True):
        sel_lambdas = []
        if "ad_lambdas_df" in st.session_state:
            df = st.session_state["ad_lambdas_df"]
            sel_lambdas = df[df["Select"] == True]["FunctionName"].tolist()
        if not sel_lambdas:
            st.warning("Select at least one Lambda function in the table above.")
        else:
            st.session_state["ad_selected_lambdas"] = sel_lambdas
            st.switch_page("pages/3_Analyse_Lambda.py")

with col_clear:
    if st.button("🧹 Clear selections", use_container_width=True):
        for k in ["ad_selected_buckets", "ad_selected_lambdas"]:
            st.session_state.pop(k, None)
        st.success("Selections cleared (discovery results retained).")