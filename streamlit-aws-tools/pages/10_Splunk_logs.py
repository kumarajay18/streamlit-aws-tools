import os
import json
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

import splunklib.client as splunk_client


# --------------------------------
# ENV — load once per server process, not on every script rerun
# --------------------------------
@st.cache_resource
def _load_env() -> None:
    """Load .env variables exactly once; avoids re-reading the file on every
    Streamlit rerun, which could trigger Streamlit's file-watcher and show a
    spurious 'Source file changed' notification."""
    load_dotenv()

_load_env()

SPLUNK_HOST = os.getenv("SPLUNK_HOST")
SPLUNK_PORT = int(os.getenv("SPLUNK_PORT", "8089"))
SPLUNK_TOKEN = os.getenv("SPLUNK_TOKEN")

DEFAULT_INDEX = "qcp_a1226_nonprod"
DEFAULT_SOURCETYPE = "kube:container:td-consumption"

st.set_page_config(page_title="ETL Trace Explorer", layout="wide")
st.title("ETL Trace Explorer")

MAX_TRACES_ALL = 200  # safety cap for "All traces" query size


# --------------------------------
# SPLUNK CONNECT
# --------------------------------
def connect():
    try:
        return splunk_client.connect(
            host=SPLUNK_HOST,
            port=SPLUNK_PORT,
            scheme="https",
            token=SPLUNK_TOKEN,
            autologin=True
        )
    except TypeError:
        # older splunklib versions
        return splunk_client.connect(
            host=SPLUNK_HOST,
            port=SPLUNK_PORT,
            scheme="https",
            splunkToken=SPLUNK_TOKEN,
            autologin=True
        )


# --------------------------------
# SPL: DISCOVER TRACE IDS
# batch_id filter ONLY here
# --------------------------------
def discovery_query(index, sourcetype, app_id, batch_id):
    spl = f'''search index={index} sourcetype="{sourcetype}" "{app_id}"'''
    if batch_id.strip():
        spl += f''' "'batch_id': {batch_id.strip()}"'''

    spl += r'''
| spath
| eval trace=lower(trim('dd.trace_id'))
| where isnotnull(trace) AND trace!=""

| stats values(trace) as traces
| mvexpand traces
| eval trace=traces
| fields - traces
| sort 0 trace
| table trace
'''
    return spl.strip()


# --------------------------------
# SPL: FETCH LOGS FOR TRACE IDS
# (NO batch_id filter here)
# --------------------------------
def logs_query(index, sourcetype, trace_ids):
    quoted = ",".join([f'"{t}"' for t in trace_ids])

    spl = f'''search index={index} sourcetype="{sourcetype}"'''
    spl += f'''
| spath
| eval trace=lower(trim('dd.trace_id'))
| eval env_raw='dd.env'
| eval env=trim(mvindex(mvdedup(env_raw),0))
| eval trace=mvindex(mvdedup(trace),0)
| eval message=mvindex(mvdedup(message),0)
| where isnotnull(trace) AND trace!=""
| search trace IN ({quoted})
| sort 0 _time
| table _time env source message trace
'''
    return spl.strip()


# --------------------------------
# RUN SPL (json_rows easiest to parse)
# --------------------------------
def run_query(service, spl, earliest, latest, debug=False, label=""):
    if debug:
        st.markdown(f"### ▶ Running: {label}")
        st.code(spl, language="spl")
        st.write({"earliest": earliest, "latest": latest})

    job = service.jobs.create(
        query=spl,
        earliest_time=earliest,
        latest_time=latest,
        exec_mode="blocking"
    )

    if debug:
        st.write("✅ Job SID:", job.sid)
        try:
            st.write("dispatchState:", job["dispatchState"])
            st.write("isDone:", job["isDone"])
            st.write("resultCount:", job["resultCount"])
        except Exception as e:
            st.warning(f"Could not read some job properties: {e}")

        try:
            msgs = job.messages
            if msgs:
                st.write("⚠️ Job messages:")
                st.json(msgs)
        except Exception:
            pass

    raw = job.results(output_mode="json_rows", count=0)
    data = json.loads(raw.read().decode("utf-8"))

    fields = data.get("fields", [])
    rows = data.get("rows", [])

    if debug:
        st.write(f"Returned fields ({len(fields)}):", fields)
        st.write(f"Returned rows: {len(rows)}")
        st.write("First 5 rows (raw):")
        st.json(rows[:5])

    return [dict(zip(fields, r)) for r in rows]


# --------------------------------
# PYTHON SAFETY: flatten lists -> string
# --------------------------------
def flatten_value(v):
    if isinstance(v, list):
        seen = set()
        cleaned = []
        for x in v:
            if x is None:
                continue
            s = str(x).strip()
            if not s:
                continue
            if s not in seen:
                seen.add(s)
                cleaned.append(s)
        if len(cleaned) == 0:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return " | ".join(cleaned)
    return v


def normalize_rows(rows):
    return [{k: flatten_value(v) for k, v in r.items()} for r in rows]


# --------------------------------
# SIDEBAR (FORM - fixes rerun blanking)
# --------------------------------
with st.sidebar:
    with st.form("splunk_form", clear_on_submit=False):
        app_id = st.text_input("Enterprise App ID (e.g., a1115)", "", key="app_id")
        batch_id = st.text_input("Batch ID (optional, e.g., 427)", "", key="batch_id")

        preset = st.radio(
            "Time Range",
            ["Last 1 hour", "Last 24 hours", "Last 3 days", "Last 7 days", "Custom"],
            index=1,
            key="preset"
        )

        now = datetime.now(timezone.utc)
        if preset == "Last 1 hour":
            earliest, latest = "-1h", "now"
        elif preset == "Last 24 hours":
            earliest, latest = "-24h", "now"
        elif preset == "Last 3 days":
            earliest, latest = "-3d", "now"
        elif preset == "Last 7 days":
            earliest, latest = "-7d", "now"
        else:
            start = st.date_input("Start", now.date(), key="start_date")
            end = st.date_input("End", now.date(), key="end_date")
            earliest = datetime.combine(start, datetime.min.time()).isoformat()
            latest = datetime.combine(end, datetime.max.time()).isoformat()

        index = st.text_input("Index", DEFAULT_INDEX, key="index")
        sourcetype = st.text_input("Sourcetype", DEFAULT_SOURCETYPE, key="sourcetype")

        debug = st.checkbox("Show Debug (SPL + SID + raw results)", value=False, key="debug")

        submitted = st.form_submit_button("Run Search")


# --------------------------------
# MAIN FLOW
# --------------------------------

# 1) Discover trace ids ONLY when submitted
if submitted:
    if not app_id.strip():
        st.warning("App ID required")
        st.stop()

    svc = connect()

    spl1 = discovery_query(index, sourcetype, app_id.strip(), batch_id)
    traces_rows = run_query(svc, spl1, earliest, latest, debug=debug, label="Discover trace IDs")

    trace_ids = sorted({str(r.get("trace")).strip() for r in traces_rows if r.get("trace")})
    trace_ids = [t for t in trace_ids if t]

    if not trace_ids:
        st.error("No trace IDs found. Try widening time range or confirm app_id/batch_id filters.")
        st.session_state.pop("trace_ids", None)
        st.session_state.pop("logs_df", None)
        st.stop()

    # ✅ persist trace IDs + search context for reruns
    st.session_state["trace_ids"] = trace_ids
    st.session_state["search_ctx"] = {
        "earliest": earliest,
        "latest": latest,
        "index": index,
        "sourcetype": sourcetype,
        "debug": debug,
    }

    # When a new search is submitted, clear old logs
    st.session_state.pop("logs_df", None)

    st.success(f"Found {len(trace_ids)} trace id(s)")


# 2) Render trace UI whenever we have trace_ids (even on reruns)
trace_ids = st.session_state.get("trace_ids", [])
ctx = st.session_state.get("search_ctx", {})

if not trace_ids:
    st.info("Enter App ID and click **Run Search** to discover trace IDs.")
    st.stop()

st.subheader("Trace IDs")
st.dataframe(pd.DataFrame({"trace": trace_ids}), use_container_width=True)

selection_mode = st.radio("Fetch logs for:", ["Single trace", "All traces"], index=0, key="sel_mode")

if selection_mode == "Single trace":
    selected_trace = st.selectbox("Select trace id", trace_ids, key="sel_trace")
    chosen_trace_ids = [selected_trace]
else:
    chosen_trace_ids = trace_ids
    if len(chosen_trace_ids) > MAX_TRACES_ALL:
        st.warning(f"Too many traces ({len(chosen_trace_ids)}). Capping to first {MAX_TRACES_ALL} to avoid query limits.")
        chosen_trace_ids = chosen_trace_ids[:MAX_TRACES_ALL]

# 3) Fetch logs on button click (stable UI)
fetch_logs = st.button("Fetch Logs", type="primary", key="fetch_logs_btn")

if fetch_logs:
    svc = connect()
    spl2 = logs_query(ctx.get("index", DEFAULT_INDEX), ctx.get("sourcetype", DEFAULT_SOURCETYPE), chosen_trace_ids)
    logs_rows = run_query(
        svc,
        spl2,
        ctx.get("earliest", "-24h"),
        ctx.get("latest", "now"),
        debug=ctx.get("debug", False),
        label="Fetch logs"
    )

    logs_rows = normalize_rows(logs_rows)
    df = pd.DataFrame(logs_rows)
    st.session_state["logs_df"] = df


# 4) Display logs if available
df = st.session_state.get("logs_df")

if isinstance(df, pd.DataFrame) and not df.empty:
    df = df.copy()

    if "_time" in df.columns:
        df["_time"] = pd.to_datetime(df["_time"], errors="coerce")
        df = df.sort_values("_time")
        df["_time"] = df["_time"].dt.strftime("%Y-%m-%d %H:%M:%S %z")

    desired_cols = ["_time", "env", "source", "message", "trace"]
    df = df[[c for c in desired_cols if c in df.columns]]

    st.subheader("Logs (sorted by timestamp)")
    st.dataframe(df, use_container_width=True)
else:
    st.info("Click **Fetch Logs** to load logs for the selected trace scope.")