# pages/5_App_Discovery.py

from __future__ import annotations

import re
from typing import Dict, List, Tuple

import streamlit as st
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

from src.aws_s3 import get_manager
from src.ui.topbar import render_topbar  # ensure your topbar module exists

st.set_page_config(page_title="App Discovery", page_icon="🔎", layout="wide")

# -----------------------------
# Sticky top bar (global session bar)
# -----------------------------

mgr = get_manager()
if not mgr.has_active_session():
    st.warning("No active AWS session. Use the top bar to log in.")
    st.stop()

# Convenience
s3 = mgr.get_s3_client()
default_region= "ap-southeast-2"

# -----------------------------
# Helpers
# -----------------------------
def normalize_appid(appid: str) -> Tuple[str, str]:
    """
    Returns (needle_full, needle_numeric)
    - needle_full: 'a1401'  (lower-cased AppId)
    - needle_numeric: '1401' (digits extracted)
    """
    a = (appid or "").strip()
    if not a:
        raise ValueError("Please provide a non-empty AppId, e.g., A1401.")
    needle_full = a.lower()
    digits = "".join(ch for ch in a if ch.isdigit())
    return needle_full, digits

def list_buckets_matching(s3_client, appid: str) -> List[Dict]:
    """
    S3 ListBuckets is account-global. We filter by substring 'appid' (lower-cased) in bucket names.
    Returns a list of dicts: {Bucket, CreationDate}
    """
    try:
        resp = s3_client.list_buckets()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError(
                "AccessDenied: The role is not allowed to list all buckets (s3:ListAllMyBuckets). "
                "Ask your platform/IAM team to allow this, or use a different discovery method (e.g., known bucket name prefix)."
            ) from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to list buckets: {e}") from e

    n_full, _ = normalize_appid(appid)
    rows = []
    for b in resp.get("Buckets", []):
        name = b.get("Name", "")
        if n_full in name.lower():
            rows.append({"Bucket": name, "CreationDate": b.get("CreationDate")})
    return rows

def list_lambda_functions_matching(lambda_client, appid: str) -> List[Dict]:
    """
    Paginates ListFunctions in the current region and filters by substring.
    We match both 'a1401' and '1401' case-insensitively.
    Returns: [{FunctionName, Runtime, LastModified, Arn}]
    """
    n_full, n_digits = normalize_appid(appid)
    needles = set([n_full, n_digits]) if n_digits else set([n_full])

    paginator = lambda_client.get_paginator("list_functions")
    try:
        pages = paginator.paginate()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError(
                "AccessDenied: Not allowed to ListFunctions in this region. "
                "Ask for lambda:ListFunctions permission in your identity policy."
            ) from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to list Lambda functions: {e}") from e

    out = []
    for page in pages:
        for fn in page.get("Functions", []):
            name = fn.get("FunctionName", "")
            lname = name.lower()
            if any(n in lname for n in needles):
                out.append({
                    "FunctionName": name,
                    "Runtime": fn.get("Runtime"),
                    "LastModified": fn.get("LastModified"),
                    "Arn": fn.get("FunctionArn")
                })
    return out

def get_lambda_env(lambda_client, function_name: str) -> Dict:
    """
    Returns env variables (dict) for a lambda function.
    If KMS is used and the role cannot decrypt, AWS may return empty or AccessDenied.
    """
    try:
        conf = lambda_client.get_function_configuration(FunctionName=function_name)
        env = conf.get("Environment", {}).get("Variables", {}) or {}
        return env
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("AccessDeniedException", "AccessDenied"):
            raise RuntimeError(
                "AccessDenied: Not allowed to read function configuration or decrypt environment variables. "
                "Ask for lambda:GetFunctionConfiguration and KMS decrypt permissions (if KMS is used)."
            ) from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to fetch function configuration: {e}") from e

def console_link_s3_bucket(bucket: str, region: str) -> str:
    # S3 console is global but supports ?region parameter
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={region}&tab=objects"

def console_link_lambda(fn_name: str, region: str) -> str:
    return f"https://{region}.console.aws.amazon.com/lambda/home?region={region}#/functions/{fn_name}?tab=configuration"

# -----------------------------
# Inputs (Sidebar or top of page)
# -----------------------------
with st.sidebar:
    st.header("🔎 App Discovery")
    appid = st.text_input("AppId (e.g., A1401)", value=st.session_state.get("app_discovery_appid", "A1401"))
    st.session_state["app_discovery_appid"] = appid

    # Lambda region override (defaults to current context region)
    lambda_region = st.text_input("Lambda Region", value=st.session_state.get("app_discovery_lambda_region", default_region))
    st.session_state["app_discovery_lambda_region"] = lambda_region

    st.markdown("---")
    btn_s3 = st.button("🔍 Find S3 Buckets", type="primary", use_container_width=True)
    btn_lambda = st.button("🔍 Find Lambda Functions", use_container_width=True)

st.title("🔎 App Discovery")

# -----------------------------
# S3 Buckets search
# -----------------------------
if btn_s3:
    if not appid.strip():
        st.error("Please enter an AppId.")
    else:
        with st.status(f"Searching S3 buckets for **{appid}** ...", expanded=True) as status:
            try:
                buckets = list_buckets_matching(s3, appid)
                if not buckets:
                    st.info("No buckets matched.")
                    st.session_state.pop("app_discovery_s3_df", None)
                else:
                    df = pd.DataFrame(buckets).sort_values("Bucket")
                    st.session_state["app_discovery_s3_df"] = df
                    st.success(f"Found {len(df)} matching bucket(s).")
                status.update(label="S3 search complete", state="complete", expanded=False)
            except Exception as e:
                st.error(str(e))

# Render S3 results if present
if "app_discovery_s3_df" in st.session_state:
    st.markdown("### 🪣 Buckets")
    df = st.session_state["app_discovery_s3_df"]
    st.dataframe(df, use_container_width=True, hide_index=True)
    # Quick console links (first 25)
    region = lambda_region or default_region
    with st.expander("Open in AWS Console (first 25)"):
        for b in df["Bucket"].tolist()[:25]:
            url = console_link_s3_bucket(b, region)
            st.markdown(f"- [{b}]({url})")

st.markdown("---")

# -----------------------------
# Lambda functions search
# -----------------------------
if btn_lambda:
    if not appid.strip():
        st.error("Please enter an AppId.")
    else:
        with st.status(f"Searching Lambda functions in **{lambda_region}** for **{appid}** ...", expanded=True) as status:
            try:
                lam = mgr.get_session().client("lambda", region_name=lambda_region)  # region-specific
                functions = list_lambda_functions_matching(lam, appid)
                if not functions:
                    st.info("No Lambda functions matched.")
                    st.session_state.pop("app_discovery_lambda_df", None)
                else:
                    df = pd.DataFrame(functions).sort_values("FunctionName")
                    st.session_state["app_discovery_lambda_df"] = df
                    st.success(f"Found {len(df)} matching function(s).")
                status.update(label="Lambda search complete", state="complete", expanded=False)
            except Exception as e:
                st.error(str(e))

# Render Lambda results if present
if "app_discovery_lambda_df" in st.session_state:
    st.markdown("### 🪄 Lambda Functions")
    df = st.session_state["app_discovery_lambda_df"]

    # Two-column: table + selection
    c_left, c_right = st.columns([1.8, 1.2])
    with c_left:
        st.dataframe(df, use_container_width=True, hide_index=True)
    with c_right:
        names = df["FunctionName"].tolist()
        fn_name = st.selectbox("Select a function", names, index=0 if names else None)

        # Console link
        if fn_name:
            st.markdown(f"[Open in AWS Console ↗]({console_link_lambda(fn_name, lambda_region)})")

        # Fetch env vars
# Fetch env vars (TABLE view)
    if st.button("🔐 Show environment variables", type="primary", disabled=(not fn_name), use_container_width=True):
        lam = mgr.get_session().client("lambda", region_name=lambda_region)
        with st.status(f"Reading environment variables for **{fn_name}** ...", expanded=True) as status:
            try:
                env = get_lambda_env(lam, fn_name)  # dict[str, str]
                if not env:
                    st.warning("No environment variables found (or access restricted).")
                else:
                    # Convert to a neat table
                    df_env = (
                        pd.DataFrame(
                            [{"Key": k, "Value": v if v is not None else ""} for k, v in env.items()]
                        )
                        .sort_values("Key")
                        .reset_index(drop=True)
                    )

                    st.markdown("#### 🔐 Environment variables")
                    st.dataframe(
                        df_env,
                        use_container_width=True,
                        hide_index=True,
                    )

                    # Offer a CSV download
                    csv_bytes = df_env.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="⬇️ Download as CSV",
                        data=csv_bytes,
                        file_name=f"{fn_name}_env_vars.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

                status.update(label="Done", state="complete", expanded=False)
            except Exception as e:
                st.error(str(e))