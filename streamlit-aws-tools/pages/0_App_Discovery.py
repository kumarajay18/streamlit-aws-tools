from __future__ import annotations

import re
from typing import Dict, List, Tuple, Optional, Set

import streamlit as st
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from src.aws_s3 import get_manager
from src.config import DEFAULT_REGION, SK
from src.ui.guards import require_aws_session
from src.ui.context import show_session_caption

# ──────────────────────────────────────────────────────────────────────────────
# Page config & constants
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="App Discovery", page_icon="🔎", layout="wide")
st.title("🔎 App Discovery")

ARTEFACT_BUCKET_SUBSTR = "deploymentfoundations-artefactsbucket"
PACKAGED_FILE_NAME = "03-lake-root.packaged.cfn.yaml"

# ──────────────────────────────────────────────────────────────────────────────
# Require active session
# ──────────────────────────────────────────────────────────────────────────────
mgr = require_aws_session()
ctx = show_session_caption()

s3 = mgr.get_s3_client()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def list_buckets_all() -> List[str]:
    try:
        resp = s3.list_buckets()
        return [b.get("Name", "") for b in resp.get("Buckets", []) if b.get("Name")]
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError("AccessDenied: Need s3:ListAllMyBuckets.") from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to list buckets: {e}") from e


def list_lambda_functions_matching(session, appids: List[str]) -> List[Dict]:
    lam = session.client("lambda", region_name=DEFAULT_REGION)

    def needles_for_appids(ids: List[str]) -> Set[str]:
        needles: Set[str] = set()
        for a in ids:
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

    needles = needles_for_appids(appids)

    paginator = lam.get_paginator("list_functions")
    try:
        pages = paginator.paginate()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError("AccessDenied: Not allowed to ListFunctions in this region.") from e
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
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={DEFAULT_REGION}&tab=objects"


def console_link_lambda(fn_name: str) -> str:
    return f"https://{DEFAULT_REGION}.console.aws.amazon.com/lambda/home?region={DEFAULT_REGION}#/functions/{fn_name}?tab=configuration"


# ──────────────────────────────────────────────────────────────────────────────
# Efficient artefact discovery (no full-bucket scan)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=300)
def find_artefact_buckets(_cache_key: str = "scan") -> List[str]:
    buckets = list_buckets_all()
    matches = [b for b in buckets if ARTEFACT_BUCKET_SUBSTR in b]
    matches.sort()
    return matches


def _list_common_prefixes(bucket: str, prefix: str) -> List[str]:
    paginator = s3.get_paginator("list_objects_v2")
    prefixes: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            p = cp.get("Prefix")
            if p:
                prefixes.append(p)
    return prefixes


def _latest_date_prefix(bucket: str) -> Optional[str]:
    date_prefixes = [p for p in _list_common_prefixes(bucket, "") if p.startswith("date=")]
    if not date_prefixes:
        return None
    return sorted(date_prefixes)[-1]


def _choose_branch_prefix(bucket: str, date_prefix: str, branch_prefs: List[str]) -> Optional[str]:
    branches = []
    for p in _list_common_prefixes(bucket, date_prefix):
        if p.startswith(date_prefix + "branch="):
            branch = p[len(date_prefix + "branch="):].strip("/")
            branches.append((branch, p))
    if not branches:
        return None
    for pref in branch_prefs:
        for b, fullp in branches:
            if b.lower() == pref.strip().lower():
                return fullp
    return sorted([fullp for _, fullp in branches])[-1]


def _max_numbered_child(bucket: str, parent_prefix: str, name: str) -> Optional[Tuple[int, str]]:
    patt = re.compile(rf"^{re.escape(parent_prefix)}{name}=(\d+)/$")
    candidates: List[Tuple[int, str]] = []
    for p in _list_common_prefixes(bucket, parent_prefix):
        m = patt.match(p)
        if m:
            candidates.append((int(m.group(1)), p))
    if not candidates:
        return None
    return max(candidates, key=lambda x: x[0])


@st.cache_data(show_spinner=False, ttl=300)
def resolve_latest_packaged_key(bucket: str, branch_prefs: Optional[List[str]] = None) -> Optional[Dict[str, str]]:
    branch_prefs = branch_prefs or ["master", "main", "develop"]

    date_prefix = _latest_date_prefix(bucket)
    if not date_prefix:
        return None

    branch_prefix = _choose_branch_prefix(bucket, date_prefix, branch_prefs)
    if not branch_prefix:
        return None

    rn = _max_numbered_child(bucket, branch_prefix, "run_number")
    if not rn:
        return None
    run_number, rn_prefix = rn

    ra = _max_numbered_child(bucket, rn_prefix, "run_attempt")
    if ra:
        run_attempt, ra_prefix = ra
        base_prefix = ra_prefix
    else:
        run_attempt = -1
        base_prefix = rn_prefix

    candidate_key = f"{base_prefix}{PACKAGED_FILE_NAME}"

    try:
        s3.head_object(Bucket=bucket, Key=candidate_key)
        return {
            "key": candidate_key,
            "date": date_prefix.split("date=")[1].strip("/"),
            "branch": branch_prefix.split("branch=")[1].strip("/"),
            "run_number": run_number,
            "run_attempt": run_attempt,
        }
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NotFound", "NoSuchKey"):
            raise

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []) or []:
            k = obj.get("Key", "")
            if k.endswith("/" + PACKAGED_FILE_NAME) or k.endswith(PACKAGED_FILE_NAME):
                return {
                    "key": k,
                    "date": date_prefix.split("date=")[1].strip("/"),
                    "branch": branch_prefix.split("branch=")[1].strip("/"),
                    "run_number": run_number,
                    "run_attempt": run_attempt,
                }
    return None


def read_s3_text(bucket: str, key: str) -> str:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp.get("Body")
        return body.read().decode("utf-8") if body else ""
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "AccessDenied":
            raise RuntimeError(f"AccessDenied: Need s3:GetObject for s3://{bucket}/{key}") from e
        if code == "NoSuchKey":
            raise RuntimeError(f"Object not found: s3://{bucket}/{key}") from e
        raise
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to read s3://{bucket}/{key}: {e}") from e


def extract_app_ids_from_cf(yaml_text: str) -> List[Tuple[str, str]]:
    """
    Returns list of (EnterpriseAppID, ResourceLogicalId).
    Tries YAML parse first; falls back to regex if PyYAML is unavailable.
    """
    apps: List[Tuple[str, str]] = []
    if yaml is not None:
        try:
            data = yaml.safe_load(yaml_text) or {}
            resources = data.get("Resources", {}) or {}
            for logical_id, res in resources.items():
                props = (res or {}).get("Properties", {})
                params = (props or {}).get("Parameters", {})
                appid = params.get("EnterpriseAppID")
                if isinstance(appid, str) and re.fullmatch(r"[aA]\d{3,4}", appid.strip()):
                    apps.append((appid.strip().lower(), logical_id))
        except Exception:
            pass

    if not apps:
        logical_id = None
        for line in yaml_text.splitlines():
            m_logical = re.match(r"^([A-Za-z0-9]+):\s*$", line)
            if m_logical:
                logical_id = m_logical.group(1)
            m_app = re.search(r"EnterpriseAppID:\s*['\"]?([aA]\d{3,4})['\"]?", line)
            if m_app and logical_id:
                apps.append((m_app.group(1).lower(), logical_id))

    seen = {}
    for appid, lid in apps:
        if appid not in seen:
            seen[appid] = lid
    return [(k, v) for k, v in sorted(seen.items(), key=lambda x: (x[0]))]


# ──────────────────────────────────────────────────────────────────────────────
# Bucket categorisation & app → bucket mapping
# ──────────────────────────────────────────────────────────────────────────────
_LAND_PATTERNS = [
    re.compile(r"(^|-)land($|-)"),
    re.compile(r"(^|-)landing($|-)"),
]
_RAW_PATTERNS = [
    re.compile(r"(^|-)raw($|-)"),
    re.compile(r"(^|-)bronze($|-)"),
]
_CURATED_PATTERNS = [
    re.compile(r"(^|-)cur($|-)"),
    re.compile(r"(^|-)curated($|-)"),
    re.compile(r"(^|-)gold($|-)"),
    re.compile(r"(^|-)silver($|-)"),
    re.compile(r"(^|-)refined($|-)"),
]


def bucket_category(name: str) -> str:
    n = (name or "").lower()
    if any(p.search(n) for p in _LAND_PATTERNS):
        return "Landing"
    if any(p.search(n) for p in _RAW_PATTERNS):
        return "Raw"
    if any(p.search(n) for p in _CURATED_PATTERNS):
        return "Curated"
    return "Other"


def appid_digits(appid: str) -> Optional[str]:
    m = re.search(r"(\d{3,4})", appid or "")
    return m.group(1) if m else None


def map_app_to_buckets(all_buckets: List[str], appids: List[str]) -> List[Dict]:
    """
    For each appid, find best matching landing/raw/curated bucket.
    Matching rule: bucket contains '-a####-' (case-insensitive), then category.
    """
    out: List[Dict] = []
    for a in appids:
        d = appid_digits(a)
        row = {"Select": True, "EnterpriseAppID": a.lower(), "LandingBucket": "", "RawBucket": "", "CuratedBucket": ""}
        if not d:
            out.append(row)
            continue
        token = f"-a{d}-"
        candidates = [b for b in all_buckets if token in b.lower()]
        for b in candidates:
            cat = bucket_category(b)
            if cat == "Landing" and not row["LandingBucket"]:
                row["LandingBucket"] = b
            elif cat == "Raw" and not row["RawBucket"]:
                row["RawBucket"] = b
            elif cat == "Curated" and not row["CuratedBucket"]:
                row["CuratedBucket"] = b
        out.append(row)
    return out


def _read_selection(evt, fallback_key: str) -> List[int]:
    """Robustly read st.dataframe selection rows across Streamlit versions."""
    try:
        if evt is not None and getattr(evt, "selection", None) is not None:
            return list(evt.selection.rows or [])
    except Exception:
        pass
    return st.session_state.get(fallback_key, [])


def _apply_select_by_rows(df: pd.DataFrame, selected_rows: List[int]) -> pd.DataFrame:
    d = df.copy().reset_index(drop=True)
    if "Select" not in d.columns:
        d.insert(0, "Select", False)
    d["Select"] = False
    if selected_rows:
        d.loc[selected_rows, "Select"] = True
    return d


# ──────────────────────────────────────────────────────────────────────────────
# SECTION — Enterprise Apps (load from latest artefact template)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("📚 Enterprise Apps (from latest artefact template)")

# Row 1: inputs aligned
i1, i2 = st.columns([2, 2], vertical_alignment="bottom")

with i1:
    if "ad_branch_prefs" not in st.session_state:
        st.session_state["ad_branch_prefs"] = "master, main, develop"
    branch_pref_raw = st.text_input(
        "Preferred branches (left→right priority)",
        key="ad_branch_prefs",
        help="Resolver picks the first available branch from this list."
    )
    branch_prefs = [b.strip() for b in branch_pref_raw.split(",") if b.strip()]

with i2:
    candidates = st.session_state.get("ad_artefact_candidates") or []
    current = st.session_state.get("ad_artefact_bucket")
    if candidates:
        chosen = st.selectbox(
            "Select artefact bucket",
            options=candidates,
            index=(candidates.index(current) if (current in candidates) else 0),
            help=f"Name contains '{ARTEFACT_BUCKET_SUBSTR}'"
        )
        if chosen and chosen != current:
            st.session_state["ad_artefact_bucket"] = chosen
    else:
        st.caption("Click **Detect Artefact Bucket** to populate options.")

# Row 2: action buttons aligned
b1, b2 = st.columns([1, 2], vertical_alignment="bottom")

with b1:
    if st.button("🔎 Detect Artefact Bucket", use_container_width=True):
        try:
            matches = find_artefact_buckets()
            if not matches:
                st.warning(f"No buckets found containing '{ARTEFACT_BUCKET_SUBSTR}'.")
                st.session_state.pop("ad_artefact_bucket", None)
                st.session_state.pop("ad_artefact_candidates", None)
            else:
                st.session_state["ad_artefact_candidates"] = matches
                if "ad_artefact_bucket" not in st.session_state:
                    st.session_state["ad_artefact_bucket"] = matches[0]
                st.success(f"Detected: {', '.join(matches)}")
        except Exception as e:
            st.error(str(e))

with b2:
    if st.button("📥 Load Latest Apps List", type="primary", use_container_width=True):
        bucket = st.session_state.get("ad_artefact_bucket")
        if not bucket:
            matches = find_artefact_buckets()
            if not matches:
                st.warning(f"No buckets found containing '{ARTEFACT_BUCKET_SUBSTR}'. Click 'Detect' first.")
            else:
                bucket = matches[0]
                st.session_state["ad_artefact_bucket"] = bucket

        if bucket:
            with st.status(f"Resolving latest {PACKAGED_FILE_NAME} in **{bucket}** …", expanded=True) as stt:
                try:
                    meta = resolve_latest_packaged_key(bucket, branch_prefs=branch_prefs)
                    if not meta:
                        st.warning(f"Could not find '{PACKAGED_FILE_NAME}' in s3://{bucket}")
                        st.session_state.pop("ad_apps_df", None)
                    else:
                        key = meta["key"]
                        txt = read_s3_text(bucket, key)
                        apps = extract_app_ids_from_cf(txt)
                        if not apps:
                            st.info("No EnterpriseAppID entries found in the latest template.")
                            st.session_state.pop("ad_apps_df", None)
                        else:
                            df_apps = pd.DataFrame(apps, columns=["EnterpriseAppID", "ResourceLogicalId"]).reset_index(drop=True)
                            df_apps.insert(0, "Select", False)
                            st.session_state["ad_apps_df"] = df_apps
                            st.session_state["ad_latest_template_meta"] = meta
                            st.session_state["ad_selected_app_rows"] = []
                            st.success(f"Loaded {len(df_apps)} app(s) from {key}")
                    stt.update(label="Apps loaded", state="complete", expanded=False)
                except Exception as e:
                    st.error(str(e))

# Apps selection (1-click row selection) + select all/clear/invert
if "ad_apps_df" in st.session_state:
    meta = st.session_state.get("ad_latest_template_meta") or {}
    bucket = st.session_state.get("ad_artefact_bucket") or "(auto)"
    st.caption(
        f"Source: **s3://{bucket}/{meta.get('key','?')}**  "
        f"(date={meta.get('date','?')}, branch={meta.get('branch','?')}, "
        f"run={meta.get('run_number','?')}, attempt={meta.get('run_attempt','?')})"
    )

    df = st.session_state["ad_apps_df"].copy().reset_index(drop=True)
    n = len(df)

    view_df = df[["EnterpriseAppID", "ResourceLogicalId"]].copy()

    evt = st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="ad_apps_table"
    )

    chosen_rows = _read_selection(evt, "ad_selected_app_rows")
    st.session_state["ad_selected_app_rows"] = chosen_rows
    df = _apply_select_by_rows(df, chosen_rows)
    st.session_state["ad_apps_df"] = df

    # Action bar
    ab_left, ab_right = st.columns([1, 1])
    with ab_left:
        if st.button("🔗 List Buckets (for Selected Apps)", type="primary", use_container_width=True, key="ad_list_buckets"):
            selected_ids = df[df["Select"] == True]["EnterpriseAppID"].tolist()
            if not selected_ids:
                st.warning("Select at least one App ID.")
            else:
                buckets = list_buckets_all()
                rows = map_app_to_buckets(buckets, selected_ids)
                df_map = pd.DataFrame(rows).reset_index(drop=True)
                st.session_state["ad_mapping_df"] = df_map
                st.session_state["ad_map_selected_rows"] = list(range(len(df_map)))  # default select all rows
                st.success(f"Mapped {len(df_map)} app(s) to buckets.")

    with ab_right:
        if st.button("🪄 List Lambda (for Selected Apps)", use_container_width=True, key="ad_list_lambda"):
            selected_ids = df[df["Select"] == True]["EnterpriseAppID"].tolist()
            if not selected_ids:
                st.warning("Select at least one App ID.")
            else:
                try:
                    fns = list_lambda_functions_matching(mgr.get_session(), selected_ids)
                    if not fns:
                        st.info("No Lambda functions matched.")
                        st.session_state.pop("ad_lambda_df", None)
                        st.session_state.pop("ad_lambda_selected_rows", None)
                    else:
                        df_l = pd.DataFrame(fns).sort_values("FunctionName").reset_index(drop=True)
                        df_l.insert(0, "Select", False)
                        st.session_state["ad_lambda_df"] = df_l
                        st.session_state["ad_lambda_selected_rows"] = []
                        st.success(f"Found {len(df_l)} function(s). See the section below.")
                except Exception as e:
                    st.error(str(e))

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# SECTION — Apps ↔ Buckets mapping (1-click selection) + actions
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("🗂️ Apps ↔ Buckets Mapping")

if "ad_mapping_df" in st.session_state:
    df_map = st.session_state["ad_mapping_df"].copy().reset_index(drop=True)
    n = len(df_map)

    # Buttons

    view_map = df_map[["EnterpriseAppID", "LandingBucket", "RawBucket", "CuratedBucket"]].copy()

    evtm = st.dataframe(
        view_map,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="ad_map_table"
    )
    chosen_rows = _read_selection(evtm, "ad_map_selected_rows")
    st.session_state["ad_map_selected_rows"] = chosen_rows

    df_map = _apply_select_by_rows(df_map, chosen_rows)
    st.session_state["ad_mapping_df"] = df_map  # persist with Select column updated

    b1, b2 = st.columns([1, 1])


    with b1:
        if st.button("📦 Analyse S3 (Selected)", use_container_width=True, key="ad_analyse_s3"):
            sel = df_map[df_map["Select"] == True].copy()
            st.session_state["ad_selected_mapping_df"] = sel.reset_index(drop=True)
            st.session_state["flow_map_selected_rows"] = st.session_state.get("ad_map_selected_rows", [])
            st.session_state["ad_selected_mapping_rows"] = sel["EnterpriseAppID"].tolist()


            buckets: List[str] = []
            for _, r in sel.iterrows():
                for col in ["LandingBucket", "RawBucket", "CuratedBucket"]:
                    b = (r.get(col) or "").strip()
                    if b:
                        buckets.append(b)
            buckets = sorted(set(buckets))
            if not buckets:
                st.warning("No buckets selected.")
            else:
                # ✅ Force Analyse S3 to refresh and not show old tables
                st.session_state["ad_selected_buckets"] = buckets
                st.session_state["s3_anal_last_buckets_sig"] = "|".join(sorted(buckets))

                # clear previous results + per-bucket editor keys
                st.session_state.pop("s3_anal_results", None)
                for k in list(st.session_state.keys()):
                    if k.startswith("s3_anal_tables_") or k.startswith("s3_anal_selrows_") or k.startswith("confirm_del_"):
                        st.session_state.pop(k, None)

                st.switch_page("pages/2_Analyse_S3.py")

    with b2:
        with st.expander("Open in AWS Console (first 25)"):
            buckets_set = set()
            for _, r in df_map[df_map["Select"] == True].iterrows():
                for col in ["LandingBucket", "RawBucket", "CuratedBucket"]:
                    b = (r.get(col) or "").strip()
                    if b:
                        buckets_set.add(b)
            for b in list(sorted(buckets_set))[:25]:
                st.markdown(f"- {console_link_s3_bucket(b)}")
else:
    st.info("No mapping yet. Load Apps and click **List Buckets (for Selected Apps)** above.")

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# SECTION — Lambda Functions (results) — 1-click selection + select all/clear/invert
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("🪄 Lambda Functions (results)")

if "ad_lambda_df" in st.session_state:
    df_lam = st.session_state["ad_lambda_df"].copy().reset_index(drop=True)
    n = len(df_lam)

    lh1, lh2, lh3 = st.columns([1, 1, 1])
    with lh1:
        if st.button("✅ Select All (Lambdas)", use_container_width=True, key="ad_lam_sel_all"):
            st.session_state["ad_lambda_selected_rows"] = list(range(n))
            st.rerun()
    with lh2:
        if st.button("🧹 Clear (Lambdas)", use_container_width=True, key="ad_lam_clear"):
            st.session_state["ad_lambda_selected_rows"] = []
            st.rerun()
    with lh3:
        if st.button("🔁 Invert (Lambdas)", use_container_width=True, key="ad_lam_invert"):
            prev = set(st.session_state.get("ad_lambda_selected_rows", []))
            st.session_state["ad_lambda_selected_rows"] = sorted(set(range(n)) - prev)
            st.rerun()

    view_lam = df_lam[["FunctionName", "Runtime", "LastModified", "Arn"]].copy()

    evtl = st.dataframe(
        view_lam,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="ad_lambda_table"
    )
    chosen_rows = _read_selection(evtl, "ad_lambda_selected_rows")
    st.session_state["ad_lambda_selected_rows"] = chosen_rows

    df_lam = _apply_select_by_rows(df_lam, chosen_rows)
    st.session_state["ad_lambda_df"] = df_lam

    if st.button("🧠 Analyse Lambda (Selected)", type="primary", use_container_width=True, key="ad_analyse_lambda"):
        sel = df_lam[df_lam["Select"] == True]["FunctionName"].tolist()
        if not sel:
            st.warning("Select at least one Lambda function.")
        else:
            st.session_state["ad_selected_lambdas"] = sel
            st.switch_page("pages/3_Analyse_Lambda.py")
else:
    st.info("Use **List Lambda (for Selected Apps)** above to fetch Lambda functions.")