# pages/3_Analyse_Lambda.py
from __future__ import annotations

import re
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError, BotoCoreError

from src.aws_s3 import get_manager

st.set_page_config(page_title="Analyse Lambda", page_icon="🧠", layout="wide")
st.title("🧠 Analyse Lambda")
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
    f"Using profile **{ctx.get('profile')}**, region **{REGION}**."
)

session = mgr.get_session()
lam = session.client("lambda", region_name=REGION)
logs_client = session.client("logs", region_name=REGION)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_lambda_env(lambda_client, function_name: str) -> Dict:
    """Return environment variables for a Lambda function."""
    try:
        conf = lambda_client.get_function_configuration(FunctionName=function_name)
        return conf.get("Environment", {}).get("Variables", {}) or {}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("AccessDeniedException", "AccessDenied"):
            raise RuntimeError("AccessDenied: Not allowed to read function configuration or decrypt env vars.") from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to fetch function configuration: {e}") from e

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

def fetch_cloudwatch_logs_for_invocation(
    session, region: str, function_name: str, request_id: Optional[str],
    start_time: datetime, end_time: datetime
) -> List[str]:
    """Fetch CloudWatch log lines for a specific invocation window (filter by RequestId if provided)."""
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
            resp = logs_client.filter_log_events(**params)
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
    """Render Lambda invoke payload as JSON or text."""
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

def get_latest_event_for_function(logs_client, function_name: str) -> Tuple[Optional[datetime], Optional[str], Optional[str], Optional[str]]:
    """
    Returns (event_time_local, message, log_group, log_stream) for the latest event of the function,
    or (None, reason, log_group, None) if not found / error.
    Uses the latest log stream by LastEventTime for efficiency.
    """
    log_group = f"/aws/lambda/{function_name}"
    try:
        resp = logs_client.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=1
        )
        streams = resp.get("logStreams", [])
        if not streams:
            return (None, "No streams/events", log_group, None)

        stream = streams[0]
        stream_name = stream.get("logStreamName")
        if not stream_name:
            return (None, "No stream name", log_group, None)

        ev = logs_client.get_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
            limit=1,
            startFromHead=False
        )
        events = ev.get("events", [])
        if not events:
            return (None, "No events in latest stream", log_group, stream_name)

        latest = events[-1]  # ensure last element
        ts_ms = latest.get("timestamp")
        msg = latest.get("message", "")
        if ts_ms is None:
            return (None, "Event without timestamp", log_group, stream_name)

        dt_local = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone()
        return (dt_local, msg, log_group, stream_name)

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        return (None, f"AWS error: {code}", log_group, None)
    except Exception as e:
        return (None, f"Error: {e}", log_group, None)

# Manual search helpers
def _tokenize_filter(text: str) -> List[str]:
    return [t for t in re.split(r"[,\s]+", (text or "").strip()) if t]

def list_lambda_functions_by_filter(session, filter_text: str, cap: int = 500) -> List[Dict]:
    """
    List Lambda functions in REGION whose names contain ALL tokens from filter_text (case-insensitive).
    cap: maximum number of matched results to return.
    """
    client = session.client("lambda", region_name=REGION)
    tokens = [t.lower() for t in _tokenize_filter(filter_text)]
    paginator = client.get_paginator("list_functions")

    results: List[Dict] = []
    try:
        for page in paginator.paginate():
            for fn in page.get("Functions", []):
                name = fn.get("FunctionName", "")
                lname = name.lower()
                if tokens and not all(t in lname for t in tokens):
                    continue
                results.append({
                    "FunctionName": name,
                    "Runtime": fn.get("Runtime"),
                    "LastModified": fn.get("LastModified"),
                    "Arn": fn.get("FunctionArn"),
                })
                if len(results) >= cap:
                    return results
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError("AccessDenied: Not allowed to ListFunctions in this region.") from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to list Lambda functions: {e}") from e
    return results

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar: Manual Lambda Search (adds/replaces analysis selection)
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔎 Manual Lambda Search")
    manual_filter = st.text_input(
        "Filter by name (substrings; comma/space separated)",
        value=st.session_state.get("lambda_manual_filter", ""),
        help="Examples: A1401, etl, batch. All tokens must be present in the name."
    )
    st.session_state["lambda_manual_filter"] = manual_filter

    max_results = st.number_input(
        "Max results",
        min_value=1, max_value=5000, value=500, step=50,
        key="lambda_manual_max"
    )

    sb1, sb2 = st.columns(2)
    with sb1:
        manual_search_btn = st.button("Search", width='stretch')
    with sb2:
        manual_clear_btn = st.button("Clear", width='stretch')

# Handle manual search actions
if manual_clear_btn:
    st.session_state.pop("lambda_manual_search_df", None)
    st.success("Cleared manual search results.")
    st.rerun()

if manual_search_btn:
    with st.status("Searching Lambda functions...", expanded=True) as s:
        try:
            rows = list_lambda_functions_by_filter(session, manual_filter, cap=int(st.session_state.get("lambda_manual_max", 500)))
            if not rows:
                st.info("No Lambda functions matched that filter.")
                st.session_state.pop("lambda_manual_search_df", None)
            else:
                df = pd.DataFrame(rows).sort_values("FunctionName").reset_index(drop=True)
                if "Select" not in df.columns:
                    df.insert(0, "Select", False)
                st.session_state["lambda_manual_search_df"] = df
                st.success(f"Found {len(df)} function(s). Select rows and add/use for analysis below.")
            s.update(label="Search complete", state="complete", expanded=False)
        except Exception as e:
            st.error(str(e))

# Render manual search results table + selection helpers + add/use
if "lambda_manual_search_df" in st.session_state:
    st.markdown("### 📚 Manual Search Results")

    # Selection helpers row
    csel1, csel2, csel3 = st.columns(3)
    with csel1:
        if st.button("✅ Select All", width='stretch'):
            df = st.session_state["lambda_manual_search_df"].copy()
            if "Select" not in df.columns:
                df.insert(0, "Select", False)
            df["Select"] = True
            st.session_state["lambda_manual_search_df"] = df
            st.rerun()
    with csel2:
        if st.button("🧹 Clear Selection", width='stretch'):
            df = st.session_state["lambda_manual_search_df"].copy()
            if "Select" not in df.columns:
                df.insert(0, "Select", False)
            df["Select"] = False
            st.session_state["lambda_manual_search_df"] = df
            st.rerun()
    with csel3:
        if st.button("🔁 Invert Selection", width='stretch'):
            df = st.session_state["lambda_manual_search_df"].copy()
            if "Select" not in df.columns:
                df.insert(0, "Select", False)
            df["Select"] = ~df["Select"].astype(bool)
            st.session_state["lambda_manual_search_df"] = df
            st.rerun()

    # Editor table
    edited = st.data_editor(
        st.session_state["lambda_manual_search_df"],
        key="lambda_manual_editor",
        width='stretch',
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False)
        }
    )
    st.session_state["lambda_manual_search_df"] = edited

    selected_in_search = edited[edited["Select"] == True]["FunctionName"].tolist()
    st.caption(f"Selected in search: **{len(selected_in_search)}**")

    ca, cb = st.columns(2)
    with ca:
        if st.button("Use Selected for Analysis (replace)", type="primary", width='stretch'):
            if not selected_in_search:
                st.warning("Select at least one function in the search table.")
            else:
                st.session_state["ad_selected_lambdas"] = selected_in_search
                st.success(f"Analysis selection replaced with {len(selected_in_search)} function(s).")
                st.rerun()
    with cb:
        if st.button("Add Selected to Analysis", width='stretch'):
            if not selected_in_search:
                st.warning("Select at least one function in the search table.")
            else:
                current = set(st.session_state.get("ad_selected_lambdas", []))
                current.update(selected_in_search)
                st.session_state["ad_selected_lambdas"] = sorted(current)
                st.success(f"Added {len(selected_in_search)} to analysis selection (now {len(current)} total).")
                st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Selected Lambdas for Analysis (from Discovery and/or Manual Search)
# ──────────────────────────────────────────────────────────────────────────────
selected_lambdas: List[str] = st.session_state.get("ad_selected_lambdas", [])
if not selected_lambdas:
    st.info("No Lambda functions selected yet. Use **App Discovery** or **Manual Lambda Search** in the sidebar.")
    st.stop()

st.markdown("### ✅ Selected Lambda functions")
sel_df = pd.DataFrame({"FunctionName": selected_lambdas})
st.dataframe(sel_df, width='stretch', hide_index=True)

if st.button("🧹 Clear current analysis selection"):
    st.session_state.pop("ad_selected_lambdas", None)
    st.success("Cleared analysis selection.")
    st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Bulk: Check Last Run for ALL selected Lambdas (retained output)
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 🔎 Last Run (All Selected Lambdas)")
col_bulk, col_clear = st.columns([1, 1])
with col_bulk:
    bulk_btn = st.button("Check Last Run (All Selected)", type="primary", width='stretch')
with col_clear:
    clear_bulk_btn = st.button("Clear Results", width='stretch')

if clear_bulk_btn:
    st.session_state.pop("lambda_anal_last_runs_df", None)
    st.success("Cleared last-run results.")

if bulk_btn:
    rows = []
    with st.status("Checking last run across selected Lambdas...", expanded=True) as status:
        for fn in selected_lambdas:
            dt_local, msg, log_group, stream_name = get_latest_event_for_function(logs_client, fn)
            rows.append({
                "FunctionName": fn,
                "LatestEventTime": dt_local.isoformat(timespec="seconds") if dt_local else "",
                "Status/Message": (msg if dt_local else (msg or "No events")),
                "LogGroup": log_group,
                "LogStream": stream_name or "",
                "ConsoleLink": f"https://{REGION}.console.aws.amazon.com/lambda/home?region={REGION}#/functions/{fn}?tab=monitoring"
            })
            st.write(f"• {fn}: {rows[-1]['LatestEventTime'] or rows[-1]['Status/Message']}")
        df = pd.DataFrame(rows)
        st.session_state["lambda_anal_last_runs_df"] = df
        status.update(label="Last run check complete", state="complete", expanded=False)

# Render retained/updated table
if "lambda_anal_last_runs_df" in st.session_state:
    st.markdown("#### 📋 Last Run Results (retained)")
    df = st.session_state["lambda_anal_last_runs_df"]
    st.dataframe(df, width='stretch', hide_index=True)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download results (CSV)",
        data=csv_bytes,
        file_name="lambda_last_run_results.csv",
        mime="text/csv",
        width='stretch'
    )

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Detailed actions for ONE selected Lambda
# ──────────────────────────────────────────────────────────────────────────────
fn_name = st.selectbox("Select a Lambda for detailed actions", selected_lambdas, index=0)
st.markdown(f"🔗 Console: https://{REGION}.console.aws.amazon.com/lambda/home?region={REGION}#/functions/{fn_name}?tab=configuration")

# Single Lambda - quick latest check
st.markdown("### ⏱️ Check Last Run (Selected Lambda)")
if st.button("Check latest log event time (selected Lambda)"):
    dt_local, msg, log_group, stream_name = get_latest_event_for_function(logs_client, fn_name)
    if dt_local:
        st.success(f"Latest event at: **{dt_local.isoformat(timespec='seconds')}**")
        st.code(msg or "", language="text")
    else:
        st.info(msg or "No log events found in the last 30 days.")

# ──────────────────────────────────────────────────────────────────────────────
# Env Variables
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 🔐 Environment Variables")
if st.button("Show env vars (for selected Lambda)"):
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
            st.dataframe(df_env, width='stretch', hide_index=True)
            csv_bytes = df_env.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download as CSV",
                data=csv_bytes,
                file_name=f"{fn_name}_env_vars.csv",
                mime="text/csv",
                width='stretch',
            )
    except Exception as e:
        st.error(str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Invoke Test + Logs (only if Lambda contains 'LambdaEtlBatch')
# ──────────────────────────────────────────────────────────────────────────────
is_etl_batch = ("lambdaetlbatch" in fn_name.lower())
if is_etl_batch:
    st.markdown("### ▶️ Invoke Test (ETL Batch)")
    st.caption("Visible because the selected function name contains **LambdaEtlBatch**.")

    default_event_text = st.session_state.get(
        "lambda_anal_test_event",
        "{\n  \"action\": \"test\",\n  \"source\": \"LambdaAnalyse\"\n}"
    )
    event_text = st.text_area(
        "Test event (JSON)",
        value=default_event_text,
        height=160,
        help="Provide a valid JSON object to send as the Lambda event. Leave empty to send {}."
    )
    st.session_state["lambda_anal_test_event"] = event_text

    dry_run = st.checkbox("Dry run (permission check only)", value=False)
    col_invoke, col_refresh = st.columns(2)
    with col_invoke:
        btn_invoke = st.button("🚀 Invoke test", type="primary", width='stretch')
    with col_refresh:
        btn_refresh = st.button("🔄 Fetch logs again for last invocation", width='stretch')

    if btn_invoke:
        try:
            payload_obj = json.loads(event_text) if event_text.strip() else {}
        except json.JSONDecodeError as je:
            st.error(f"Invalid JSON for test event: {je}")
        else:
            window_pad = timedelta(minutes=2)
            start_time = datetime.now(timezone.utc) - window_pad

            with st.status(f"Invoking **{fn_name}** in **{REGION}** ...", expanded=True) as status:
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

                    st.session_state["lambda_anal_last_invocation"] = {
                        "fn_name": fn_name,
                        "region": REGION,
                        "request_id": request_id,
                        "start_time": start_time.isoformat(),
                        "is_dry_run": bool(dry_run),
                    }

                    if not dry_run:
                        st.markdown("#### 🔎 CloudWatch logs for this invocation")
                        logs_lines = fetch_logs_with_retry(
                            session=session,
                            region=REGION,
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

    if btn_refresh:
        inv = st.session_state.get("lambda_anal_last_invocation")
        if not inv:
            st.warning("No last invocation context to refresh.")
        else:
            if inv.get("is_dry_run"):
                st.info("Last invocation was a DryRun — no logs to fetch.")
            else:
                if inv.get("fn_name") != fn_name or inv.get("region") != REGION:
                    st.warning("The selected function changed since last run. Please invoke again.")
                else:
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

# ──────────────────────────────────────────────────────────────────────────────
# Update NOS Table — only for LambdaEtlBatch
# ──────────────────────────────────────────────────────────────────────────────
if is_etl_batch:
    st.markdown("### 🛠️ Update NOS Table (ETL Batch)")
    if st.button("Modify NOS Table (from Lambda env PIPELINE_ID)", type="primary"):
        try:
            env = get_lambda_env(lam, fn_name)
            pipeline_id = env.get("PIPELINE_ID") or env.get("PipelineId")
            if not pipeline_id:
                st.error("PIPELINE_ID not found in Lambda environment variables.")
            else:
                st.session_state["nos_pipeline_id"] = pipeline_id
                st.session_state["nos_dynamo_region"] = REGION
                try:
                    st.switch_page("pages/9_Modify_NOS_Table.py")
                except Exception:
                    st.success("Pipeline ID captured. Navigate to the NOS page from the sidebar if not auto-routed.")
                    st.write("**Target page:** `Modify NOS Table`")
        except Exception as e:
            st.error(f"Failed to read Lambda env vars: {e}")