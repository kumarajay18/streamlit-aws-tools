# pages/5_App_Discovery.py

from __future__ import annotations

import re
import json
import base64
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta, timezone

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
default_region = "ap-southeast-2"

# -----------------------------
# Helpers
# -----------------------------
def normalize_appid(appid: str) -> Tuple[str, str]:
    a = (appid or "").strip()
    if not a:
        raise ValueError("Please provide a non-empty AppId, e.g., A1401.")
    needle_full = a.lower()
    digits = "".join(ch for ch in a if ch.isdigit())
    return needle_full, digits


def list_buckets_matching(s3_client, appid: str) -> List[Dict]:
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
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={region}&tab=objects"


def console_link_lambda(fn_name: str, region: str) -> str:
    return f"https://{region}.console.aws.amazon.com/lambda/home?region={region}#/functions/{fn_name}?tab=configuration"


def extract_request_id_from_tail(log_tail: str) -> Optional[str]:
    if not log_tail:
        return None
    m = re.search(r"REPORT\s+RequestId:\s*([0-9a-fA-F-]{10,})", log_tail)
    if m:
        return m.group(1)
    m = re.search(r"RequestId:\s*([0-9a-fA-F-]{10,})", log_tail)
    if m:
        return m.group(1)
    return None


def fetch_cloudwatch_logs_for_invocation(session, region: str, function_name: str, request_id: Optional[str],
                                         start_time: datetime, end_time: datetime) -> List[str]:
    logs_client = session.client("logs", region_name=region)
    log_group = f"/aws/lambda/{function_name}"

    params = {
        "logGroupName": log_group,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "interleaved": True,
        "limit": 10000,
    }
    if request_id:
        params["filterPattern"] = f'"{request_id}"'

    lines: List[str] = []
    next_token = None
    try:
        while True:
            if next_token:
                params["nextToken"] = next_token
            resp = session.client("logs", region_name=region).filter_log_events(**params)
            events = resp.get("events", [])
            for e in events:
                ts = e.get("timestamp")
                msg = e.get("message", "")
                if msg.endswith("\n"):
                    msg = msg[:-1]
                dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone()
                lines.append(f"[{dt.isoformat(timespec='seconds')}] {msg}")
            next_token = resp.get("nextToken")
            if not next_token:
                break
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("AccessDeniedException", "AccessDenied"):
            lines.append("⚠️ AccessDenied: Missing logs:FilterLogEvents on the Lambda log group.")
        else:
            lines.append(f"⚠️ CloudWatch Logs error: {code or str(e)}")
    except BotoCoreError as e:
        lines.append(f"⚠️ Boto core error while reading logs: {e}")
    except Exception as e:
        lines.append(f"⚠️ Unexpected error while reading logs: {e}")

    return lines


def fetch_logs_with_retry(session, region: str, function_name: str, request_id: Optional[str],
                          start_time: datetime, attempts: int = 5, delay_sec: float = 2.0) -> List[str]:
    out: List[str] = []
    for i in range(1, attempts + 1):
        st.info(f"⏳ Waiting for CloudWatch logs (attempt {i}/{attempts}) ...")
        end_time = datetime.now(timezone.utc) + timedelta(minutes=2)
        out = fetch_cloudwatch_logs_for_invocation(
            session=session,
            region=region,
            function_name=function_name,
            request_id=request_id,
            start_time=start_time,
            end_time=end_time,
        )
        if out and (not request_id or any(request_id in line for line in out)):
            break
        if i < attempts:
            import time as _time
            _time.sleep(delay_sec)
    return out


def render_payload_response(body_stream) -> None:
    if body_stream is None:
        st.info("Function returned no payload.")
        return
    raw = body_stream.read()
    if not raw:
        st.info("Function returned an empty payload.")
        return
    try:
        text = raw.decode("utf-8", errors="replace")
        parsed = json.loads(text)
        with st.expander("📦 Response (JSON)"):
            st.json(parsed)
    except Exception:
        with st.expander("📦 Response (text)"):
            try:
                st.code(raw.decode("utf-8", errors="replace"))
            except Exception:
                st.code(str(raw))

# -----------------------------
# Inputs (Sidebar)
# -----------------------------
with st.sidebar:
    st.header("🔎 App Discovery")
    appid = st.text_input("AppId (e.g., A1401)", value=st.session_state.get("app_discovery_appid", "A1401"))
    st.session_state["app_discovery_appid"] = appid

    lambda_region = st.text_input(
        "Lambda Region",
        value=st.session_state.get("app_discovery_lambda_region", default_region)
    )
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

if "app_discovery_s3_df" in st.session_state:
    st.markdown("### 🪣 Buckets")
    df = st.session_state["app_discovery_s3_df"]
    st.dataframe(df, use_container_width=True, hide_index=True)
    region = lambda_region or default_region
    with st.expander("Open in AWS Console (first 25)"):
        for b in df["Bucket"].tolist()[:25]:
            url = console_link_s3_bucket(b, region)
            st.markdown(f"- {url}")

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
                lam = mgr.get_session().client("lambda", region_name=lambda_region)
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

# -----------------------------
# Render Lambda block
# -----------------------------
fn_name: Optional[str] = None
if "app_discovery_lambda_df" in st.session_state:
    st.markdown("### 🪄 Lambda Functions")
    df = st.session_state["app_discovery_lambda_df"]

    c_left, c_right = st.columns([1.8, 1.2])
    with c_left:
        st.dataframe(df, use_container_width=True, hide_index=True)
    with c_right:
        names = df["FunctionName"].tolist()
        fn_name = st.selectbox("Select a function", names, index=0 if names else None)
        if fn_name:
            url = console_link_lambda(fn_name, lambda_region)
            st.markdown(f"{url}")

    # -----------------------------
    # Test Lambda — only for LambdaEtlBatch
    # -----------------------------
    is_etl_batch = bool(fn_name and ("lambdaetlbatch" in fn_name.lower()))
    if is_etl_batch:
        st.markdown("### ▶️ Test Lambda (ad‑hoc)")
        st.caption(
            "Visible because the selected function name contains **LambdaEtlBatch**. "
            "Sends a JSON event to the function (or DryRun) and fetches **full CloudWatch logs** for this run."
        )
        default_event_text = st.session_state.get(
            "app_discovery_lambda_test_event",
            "{\n  \"action\": \"test\",\n  \"source\": \"AppDiscovery\"\n}"
        )
        event_text = st.text_area(
            "Test event (JSON)",
            value=default_event_text,
            height=160,
            help="Provide a valid JSON object to send as the Lambda event. Leave empty to send {}."
        )
        st.session_state["app_discovery_lambda_test_event"] = event_text

        col_test_left, col_test_right = st.columns([1, 1])
        with col_test_left:
            dry_run = st.checkbox("Dry run (permission check only)", value=False)
        with col_test_right:
            btn_invoke = st.button("🚀 Invoke test", type="primary", use_container_width=True, disabled=not bool(fn_name))

        refresh_label = "🔄 Fetch logs again for last invocation"
        do_refresh = st.button(refresh_label, key="app_discovery_refresh_logs_btn")

        last_inv = st.session_state.get("app_discovery_last_invocation")

        if btn_invoke and fn_name:
            try:
                payload_obj = json.loads(event_text) if event_text.strip() else {}
            except json.JSONDecodeError as je:
                st.error(f"Invalid JSON for test event: {je}")
            else:
                session = mgr.get_session()
                lam = session.client("lambda", region_name=lambda_region)
                window_pad = timedelta(minutes=2)
                start_time = datetime.now(timezone.utc) - window_pad

                with st.status(f"Invoking **{fn_name}** in **{lambda_region}** ...", expanded=True) as status:
                    request_id = None
                    try:
                        if dry_run:
                            resp = lam.invoke(FunctionName=fn_name, InvocationType="DryRun")
                            st.success("DryRun succeeded (lambda:InvokeFunction is allowed).")
                            st.json({
                                "StatusCode": resp.get("StatusCode"),
                                "ExecutedVersion": resp.get("ExecutedVersion"),
                            })
                        else:
                            resp = lam.invoke(
                                FunctionName=fn_name,
                                InvocationType="RequestResponse",
                                LogType="Tail",
                                Payload=json.dumps(payload_obj).encode("utf-8"),
                            )
                            st.write("**Result**")
                            st.json({
                                "StatusCode": resp.get("StatusCode"),
                                "ExecutedVersion": resp.get("ExecutedVersion"),
                            })
                            log_b64 = resp.get("LogResult")
                            log_tail = ""
                            if log_b64:
                                try:
                                    log_tail = base64.b64decode(log_b64).decode("utf-8", errors="replace")
                                    with st.expander("📜 Execution logs (tail from Invoke)"):
                                        st.code(log_tail, language="text")
                                except Exception:
                                    st.warning("Could not decode LogResult.")
                            request_id = extract_request_id_from_tail(log_tail)
                            body = resp.get("Payload")
                            render_payload_response(body)

                        st.session_state["app_discovery_last_invocation"] = {
                            "fn_name": fn_name,
                            "region": lambda_region,
                            "request_id": request_id,
                            "start_time": start_time.isoformat(),
                            "is_dry_run": bool(dry_run),
                        }

                        if not dry_run:
                            st.markdown("#### 🔎 CloudWatch logs for this invocation")
                            logs_lines = fetch_logs_with_retry(
                                session=session,
                                region=lambda_region,
                                function_name=fn_name,
                                request_id=request_id,
                                start_time=start_time,
                                attempts=5,
                                delay_sec=2.0,
                            )
                            if logs_lines:
                                with st.expander("📜 CloudWatch Logs (full)"):
                                    st.code("\n".join(logs_lines), language="text")
                            else:
                                st.info("No CloudWatch log events found yet. Use the button above to try again in a moment.")

                        status.update(label="Invocation complete", state="complete", expanded=False)

                    except ClientError as e:
                        code = e.response.get("Error", {}).get("Code")
                        if code in ("AccessDeniedException", "AccessDenied"):
                            st.error(
                                "AccessDenied: Not allowed to invoke this function or read its logs. "
                                "Ask for **lambda:InvokeFunction** and **logs:FilterLogEvents** on the log group."
                            )
                        else:
                            st.error(f"AWS error: {code or str(e)}")
                    except BotoCoreError as e:
                        st.error(f"Boto core error: {e}")
                    except Exception as e:
                        st.error(f"Unexpected error: {e}")

        if do_refresh:
            inv = st.session_state.get("app_discovery_last_invocation")
            if not inv:
                st.warning("No last invocation context to refresh.")
            else:
                if inv.get("is_dry_run"):
                    st.info("Last invocation was a DryRun — no logs to fetch.")
                else:
                    if inv.get("fn_name") != fn_name or inv.get("region") != lambda_region:
                        st.warning("The selected function/region changed since last run. Please invoke again.")
                    else:
                        session = mgr.get_session()
                        try:
                            start_time = datetime.fromisoformat(inv["start_time"])
                            if start_time.tzinfo is None:
                                start_time = start_time.replace(tzinfo=timezone.utc)
                        except Exception:
                            start_time = datetime.now(timezone.utc) - timedelta(minutes=5)
                        st.markdown("#### 🔁 Refreshing CloudWatch logs for last invocation")
                        logs_lines = fetch_logs_with_retry(
                            session=session,
                            region=inv.get("region"),
                            function_name=inv.get("fn_name"),
                            request_id=inv.get("request_id"),
                            start_time=start_time,
                            attempts=5,
                            delay_sec=2.0,
                        )
                        if logs_lines:
                            with st.expander("📜 CloudWatch Logs (full) — refreshed"):
                                st.code("\n".join(logs_lines), language="text")
                        else:
                            st.info("Still no log events found.")

    # -----------------------------
    # Modify NOS Table — only for LambdaRawToCurated
    # -----------------------------
    is_raw_to_curated = bool(fn_name and ("lambdarawtocurated" in fn_name.lower()))
    if is_raw_to_curated:
        st.markdown("### 🛠️ Modify NOS Table")
        st.caption("Enabled because this Lambda contains **LambdaRawToCurated**.")
        if st.button("Modify NOS Table", type="primary", use_container_width=True):
            # Fetch PIPELINE_ID from Lambda env variables then navigate
            try:
                lam = mgr.get_session().client("lambda", region_name=lambda_region)
                env = get_lambda_env(lam, fn_name)
                pipeline_id = env.get("PIPELINE_ID") or env.get("PipelineId")
                if not pipeline_id:
                    st.error("PIPELINE_ID not found in Lambda environment variables.")
                else:
                    st.session_state["nos_pipeline_id"] = pipeline_id
                    st.session_state["nos_dynamo_region"] = lambda_region
                    try:
                        st.switch_page("pages/9_Modify_NOS_Table.py")
                    except Exception:
                        st.success("Pipeline ID captured. Navigate to the new page from the sidebar if not auto-routed.")
                        st.write("**Target page:** `Modify NOS Table`")
            except Exception as e:
                st.error(f"Failed to read Lambda env vars: {e}")

# -----------------------------
# Show env vars
# -----------------------------
if 'fn_name' in locals() and fn_name:
    if st.button("🔐 Show environment variables", type="primary", use_container_width=True):
        lam = mgr.get_session().client("lambda", region_name=lambda_region)
        with st.status(f"Reading environment variables for **{fn_name}** ...", expanded=True) as status:
            try:
                env = get_lambda_env(lam, fn_name)
                if not env:
                    st.warning("No environment variables found (or access restricted).")
                else:
                    df_env = (
                        pd.DataFrame(
                            [{"Key": k, "Value": v if v is not None else ""} for k, v in env.items()]
                        )
                        .sort_values("Key")
                        .reset_index(drop=True)
                    )
                    st.markdown("#### 🔐 Environment variables")
                    st.dataframe(df_env, use_container_width=True, hide_index=True)
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