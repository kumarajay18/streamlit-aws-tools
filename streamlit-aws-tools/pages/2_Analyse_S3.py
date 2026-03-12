from __future__ import annotations

import os
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError, BotoCoreError

from src.aws_s3 import get_manager
from src.config import QA_LIST_CAP, RAW_LAST_N_DATES, CURATED_LAST_N_BATCHES, SK
from src.core.common import S3Utils, get_default_date_range, extract_file_extension, SYDNEY_TZ
from src.core.s3_browser import S3Browser
from src.core.s3_downloader import S3Downloader
from src.core.s3_deleter import S3Deleter
from src.core.qa_inspector import QAInspector
from src.ui.guards import require_aws_session
from src.ui.context import show_session_caption

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="S3 Flow (Analyse + QA)", page_icon="🧭", layout="wide")
st.title("🧭 S3 Flow — Analyse + ETL QA")

# ──────────────────────────────────────────────────────────────────────────────
# Require active session (replaces 4-line copy-paste guard)
# ──────────────────────────────────────────────────────────────────────────────
mgr = require_aws_session()
ctx = show_session_caption()

s3 = mgr.get_s3_client()
browser = S3Browser(s3)
downloader = S3Downloader(s3)
deleter = S3Deleter(s3)
qa = QAInspector(boto3_client=s3, boto3_session=mgr.get_session(), s3_endpoint_url=ctx.get("s3_endpoint_url"))

# ──────────────────────────────────────────────────────────────────────────────
# Helpers (shared)
# ──────────────────────────────────────────────────────────────────────────────
# get_default_date_range() and extract_file_extension() have been moved to
# src/core/common.py and are imported above.  All call-sites in this file now
# use those imports directly.
def _build_entity_paths_df(source: str) -> Optional[pd.DataFrame]:
    """
    source: 'raw' or 'curated'
    Returns a DataFrame with columns: [AppID, BucketType, Bucket, Entity, S3 Path]
    """
    if SK.QA_MAPPING_DF not in st.session_state:
        st.warning("No mapping available. Use the mapping table above to select rows.")
        return None

    df_map_local = st.session_state[SK.QA_MAPPING_DF]
    sel = df_map_local[df_map_local["Select"] == True].copy()
    if sel.empty:
        st.warning("Select at least one row in the mapping table (click rows to select).")
        return None

    rows: List[Dict] = []
    with st.status(f"Listing {source.upper()} entity paths…", expanded=False):
        for _, r in sel.iterrows():
            app = r.get("EnterpriseAppID")
            if source == "raw":
                rb = (r.get("RawBucket") or "").strip()
                if not rb:
                    continue
                # Use your helper to get entity names under raw ("entity/<name>/")
                entities = _list_entities_under_raw(rb)
                for ent in entities:
                    rows.append({
                        "AppID": app,
                        "BucketType": "Raw",
                        "Bucket": rb,
                        "Entity": ent,
                        "S3 Path": f"s3://{rb}/entity/{ent}/",
                    })
            elif source == "curated":
                cb = (r.get("CuratedBucket") or "").strip()
                if not cb:
                    continue
                # Use your helper to get entity names at root ("<entity>/")
                entities = _list_entities_under_curated(cb)
                for ent in entities:
                    rows.append({
                        "AppID": app,
                        "BucketType": "Curated",
                        "Bucket": cb,
                        "Entity": ent,
                        "S3 Path": f"s3://{cb}/{ent}/",
                    })
            else:
                st.error("Invalid source for entity listing.")
                return None

    if not rows:
        st.info("No entities found for current selection and filters.")
        return pd.DataFrame(columns=["AppID", "BucketType", "Bucket", "Entity", "S3 Path"])

    return pd.DataFrame(rows).sort_values(["BucketType", "AppID", "Entity"]).reset_index(drop=True)

def _list_common_prefixes(bucket: str, prefix: str) -> List[str]:
    paginator = s3.get_paginator("list_objects_v2")
    prefixes: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            p = cp.get("Prefix")
            if p:
                prefixes.append(p)
    return prefixes

def _latest_object_time_filtered(
    bucket: str,
    prefix: str,
    versions_mode: bool,
    include_delete_markers: bool,
    start_utc: Optional[datetime],
    end_utc: Optional[datetime],
    cap_per_prefix: int,
) -> Optional[datetime]:
    rows = []
    if not bucket:
        return None

    if versions_mode:
        rows = browser.list_object_versions(
            bucket=bucket,
            prefix=prefix,
            cap=min(QA_LIST_CAP, cap_per_prefix),
            start_utc=start_utc,
            end_utc=end_utc,
            include_delete_markers=include_delete_markers,
        )
        rows = [r for r in rows if not r.get("IsDeleteMarker")]
    else:
        rows = browser.list_objects(
            bucket=bucket,
            prefix=prefix,
            cap=min(QA_LIST_CAP, cap_per_prefix),
            start_utc=start_utc,
            end_utc=end_utc,
        )

    if not rows:
        return None

    times = [r.get("LastModified") for r in rows if isinstance(r.get("LastModified"), datetime)]
    return max(times) if times else None

def _list_entities_under_raw(bucket: str) -> List[str]:
    base = "entity/"
    ents = []
    for p in _list_common_prefixes(bucket, base):
        m = re.match(rf"^{re.escape(base)}([^/]+)/$", p)
        if m:
            ents.append(m.group(1))
    return sorted(ents)

def _find_last_n_dates_with_data(
    bucket: str,
    entity_base: str,
    n: int,
    versions_mode: bool,
    include_delete_markers: bool,
    start_utc: Optional[datetime],
    end_utc: Optional[datetime],
    cap_per_prefix: int,
) -> List[str]:
    years = []
    for yp in _list_common_prefixes(bucket, entity_base):
        m = re.match(rf"^{re.escape(entity_base)}(\d{{4}})/$", yp)
        if m:
            years.append(m.group(1))
    years = sorted(years, reverse=True)

    results: List[str] = []
    for y in years:
        mprefix = f"{entity_base}{y}/"
        months = []
        for mp in _list_common_prefixes(bucket, mprefix):
            m = re.match(rf"^{re.escape(mprefix)}(\d{{2}})/$", mp)
            if m:
                months.append(m.group(1))
        months = sorted(months, reverse=True)

        for mo in months:
            dprefix = f"{mprefix}{mo}/"
            days = []
            for dp in _list_common_prefixes(bucket, dprefix):
                m = re.match(rf"^{re.escape(dprefix)}(\d{{2}})/$", dp)
                if m:
                    days.append(m.group(1))
            days = sorted(days, reverse=True)

            for d in days:
                pref = f"{entity_base}{y}/{mo}/{d}/"
                if versions_mode:
                    rows = browser.list_object_versions(
                        bucket=bucket,
                        prefix=pref,
                        cap=cap_per_prefix,
                        start_utc=start_utc,
                        end_utc=end_utc,
                        include_delete_markers=include_delete_markers,
                    )
                    rows = [r for r in rows if not r.get("IsDeleteMarker")]
                else:
                    rows = browser.list_objects(
                        bucket=bucket,
                        prefix=pref,
                        cap=cap_per_prefix,
                        start_utc=start_utc,
                        end_utc=end_utc,
                    )
                if rows:
                    results.append(f"{y}/{mo}/{d}")
                    if len(results) >= n:
                        return results
    return results

def _list_entities_under_curated(bucket: str) -> List[str]:
    ents = []
    for p in _list_common_prefixes(bucket, ""):
        m = re.match(r"^([^/]+)/$", p)
        if m:
            ents.append(m.group(1))
    return sorted(ents)

def _find_last_n_batches_with_data(
    bucket: str,
    entity: str,
    n: int,
    versions_mode: bool,
    include_delete_markers: bool,
    start_utc: Optional[datetime],
    end_utc: Optional[datetime],
    cap_per_prefix: int,
) -> List[int]:
    ep = f"{entity}/"
    batches: List[int] = []
    for p in _list_common_prefixes(bucket, ep):
        m = re.match(rf"^{re.escape(ep)}(\d+)/$", p)
        if not m:
            continue
        try:
            b = int(m.group(1))
        except Exception:
            continue

        pref = f"{entity}/{b}/"
        if versions_mode:
            rows = browser.list_object_versions(
                bucket=bucket,
                prefix=pref,
                cap=cap_per_prefix,
                start_utc=start_utc,
                end_utc=end_utc,
                include_delete_markers=include_delete_markers,
            )
            rows = [r for r in rows if not r.get("IsDeleteMarker")]
        else:
            rows = browser.list_objects(
                bucket=bucket,
                prefix=pref,
                cap=cap_per_prefix,
                start_utc=start_utc,
                end_utc=end_utc,
            )
        if rows:
            batches.append(b)

    batches.sort(reverse=True)
    return batches[:n]

def _sample_row_in_prefix(
    bucket: str,
    prefix: str,
    versions_mode: bool,
    include_delete_markers: bool,
    start_utc: Optional[datetime],
    end_utc: Optional[datetime],
    cap_per_prefix: int,
) -> Optional[Dict]:
    if versions_mode:
        rows = browser.list_object_versions(
            bucket=bucket,
            prefix=prefix,
            cap=cap_per_prefix,
            start_utc=start_utc,
            end_utc=end_utc,
            include_delete_markers=include_delete_markers,
        )
        rows = [r for r in rows if not r.get("IsDeleteMarker")]
    else:
        rows = browser.list_objects(
            bucket=bucket,
            prefix=prefix,
            cap=cap_per_prefix,
            start_utc=start_utc,
            end_utc=end_utc,
        )
    return rows[0] if rows else None

def _get_mapping_df() -> Optional[pd.DataFrame]:
    # ✅ 1) Prefer "selected mapping" passed from App Discovery (if present)
    if SK.AD_SELECTED_MAPPING in st.session_state and isinstance(st.session_state[SK.AD_SELECTED_MAPPING], pd.DataFrame):
        return st.session_state[SK.AD_SELECTED_MAPPING]

    # ✅ 2) Prefer qa mapping if present; else discovery mapping if present
    if SK.QA_MAPPING_DF in st.session_state and isinstance(st.session_state[SK.QA_MAPPING_DF], pd.DataFrame):
        return st.session_state[SK.QA_MAPPING_DF]
    if SK.AD_MAPPING_DF in st.session_state and isinstance(st.session_state[SK.AD_MAPPING_DF], pd.DataFrame):
        return st.session_state[SK.AD_MAPPING_DF]
    return None

def _ensure_mapping_select_col(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "Select" not in d.columns:
        d.insert(0, "Select", True)
    d["Select"] = d["Select"].astype(bool)
    return d.reset_index(drop=True)

def _selected_buckets_from_mapping(df_map: pd.DataFrame, bucket_col: str) -> List[str]:
    sel = df_map[df_map["Select"] == True].copy()
    buckets: List[str] = []
    if sel.empty:
        return []
    for _, r in sel.iterrows():
        b = (r.get(bucket_col) or "").strip()
        if b:
            buckets.append(b)
    return sorted(set(buckets))

def _fmt_bucket_type_label(col: str) -> str:
    return {
        "LandingBucket": "Landing",
        "RawBucket": "Raw",
        "CuratedBucket": "Curated",
    }.get(col, col)

# ──────────────────────────────────────────────────────────────────────────────
# Global sidebar (shared filters for BOTH Analyse + QA)
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Shared Filters (Analyse + QA)")

    # Prefix
    prefix_all = st.text_input(
        "Prefix (applies to listings)",
        value=st.session_state.get(SK.FLOW_PREFIX, ""),
        placeholder="e.g., entity/ or folder/subfolder/"
    )
    st.session_state[SK.FLOW_PREFIX] = prefix_all

    # Versions + delete markers
    versions_mode = st.checkbox(
        "List object versions",
        value=st.session_state.get(SK.FLOW_VERSIONS, False),
        help="If enabled, listings show object versions. Use delete marker toggle below."
    )
    st.session_state[SK.FLOW_VERSIONS] = versions_mode

    include_delete_markers = st.checkbox(
        "Include delete markers (versions only)",
        value=st.session_state.get(SK.FLOW_DEL_MARKERS, False),
        disabled=not versions_mode
    )
    st.session_state[SK.FLOW_DEL_MARKERS] = include_delete_markers

    # Time range
    st.markdown("### Time filter (Sydney time)")
    enable_time_filter = st.checkbox(
        "Enable datetime range",
        value=st.session_state.get(SK.FLOW_TIME_ENABLED, False)
    )
    st.session_state[SK.FLOW_TIME_ENABLED] = enable_time_filter

    default_start, default_end = get_default_date_range()
    start_dt = st.datetime_input(
        "Start (Sydney time)",
        value=st.session_state.get(SK.FLOW_START_DT, default_start),
        disabled=not enable_time_filter
    )
    end_dt = st.datetime_input(
        "End (Sydney time)",
        value=st.session_state.get(SK.FLOW_END_DT, default_end),
        disabled=not enable_time_filter
    )
    st.session_state[SK.FLOW_START_DT] = start_dt
    st.session_state[SK.FLOW_END_DT] = end_dt

    start_utc = S3Utils.to_utc(start_dt) if enable_time_filter else None
    end_utc = S3Utils.to_utc(end_dt) if enable_time_filter else None

    st.markdown("---")
    max_items = st.number_input(
        "Max results per bucket",
        min_value=1, max_value=10000, value=int(st.session_state.get(SK.FLOW_MAX_ITEMS, 1000)), step=100
    )
    st.session_state[SK.FLOW_MAX_ITEMS] = int(max_items)

    cap_per_prefix = st.number_input(
        "QA cap per prefix",
        min_value=100, max_value=10000, value=int(st.session_state.get(SK.FLOW_CAP_PER_PREFIX, 1000)), step=100,
        help="Used by QA checks to cap scanning under each prefix."
    )
    st.session_state[SK.FLOW_CAP_PER_PREFIX] = int(cap_per_prefix)

    # Download settings (Analyse only)
    st.markdown("---")
    dest_default = st.session_state.get(SK.FLOW_DOWNLOAD_DIR, str((Path.cwd() / "downloads").resolve()))
    dest_dir_str = st.text_input("Download destination (local path)", value=dest_default)
    st.session_state[SK.FLOW_DOWNLOAD_DIR] = dest_dir_str
    preserve_structure = st.checkbox("Preserve folder structure", value=st.session_state.get(SK.FLOW_PRESERVE, True))
    st.session_state[SK.FLOW_PRESERVE] = preserve_structure

# ──────────────────────────────────────────────────────────────────────────────
# Tabs: Analyse | QA
# ──────────────────────────────────────────────────────────────────────────────
_TAB_LABELS = ["📦 Analyse S3", "🧪 ETL QA Tools"]
_TAB_KEYS   = ["analyse", "qa"]

tab_analyse, tab_qa = st.tabs(_TAB_LABELS)

# ──────────────────────────────────────────────────────────────────────────────
# TAB 1: Analyse S3 (flow)
# ──────────────────────────────────────────────────────────────────────────────
with tab_analyse:
    st.subheader("📦 Analyse S3 — Bucket Listings / Download / Delete / Latest")

    mapping_df = _get_mapping_df()
    if mapping_df is None:
        # fallback to simple bucket list from discovery if provided
        selected_buckets_fallback = st.session_state.get(SK.AD_SELECTED_BUCKETS, [])
        if not selected_buckets_fallback:
            st.info("No mapping/buckets received yet. Go to **App Discovery** and select apps/buckets.")
            st.stop()
        st.write("**Selected buckets (fallback):** ", ", ".join(selected_buckets_fallback))
        st.warning("Mapping not found; listing buttons will use the fallback bucket list.")

        # Provide bucket-type buttons still, all map to fallback
        buckets_landing = selected_buckets_fallback
        buckets_raw = selected_buckets_fallback
        buckets_curated = selected_buckets_fallback
    else:
        # Show mapping table (1-click row selection, no checkbox double click)
        st.markdown("#### 🗂️ Apps ↔ Buckets Mapping (selection drives bucket lists)")
        df_map = _ensure_mapping_select_col(mapping_df)

        nrows = len(df_map)

        # Restore previously-selected rows (default to all rows selected on first load)
        sel_rows = st.session_state.get(
            SK.FLOW_MAP_SELECTED,
            st.session_state.get(SK.AD_MAP_SELECTED, list(range(nrows)))
        ) or list(range(nrows))

        view_map = df_map[["EnterpriseAppID", "LandingBucket", "RawBucket", "CuratedBucket"]].copy()

        # Single-click fix: pass `default` so the widget remembers its selection
        # across reruns (without this, every rerun resets the selection and a
        # second click is needed to actually register the row).
        evt_map = st.dataframe(
            view_map,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key="flow_map_df",
        )
        chosen_rows = list(evt_map.selection.rows or []) if evt_map and getattr(evt_map, "selection", None) else sel_rows
        st.session_state[SK.FLOW_MAP_SELECTED] = chosen_rows

        # sync Select column back into mapping
        df_map["Select"] = False
        if chosen_rows:
            df_map.loc[chosen_rows, "Select"] = True

        # persist back to whichever mapping key exists
        if SK.QA_MAPPING_DF in st.session_state:
            st.session_state[SK.QA_MAPPING_DF] = df_map
        elif SK.AD_MAPPING_DF in st.session_state:
            st.session_state[SK.AD_MAPPING_DF] = df_map

        buckets_landing = _selected_buckets_from_mapping(df_map, "LandingBucket")
        buckets_raw = _selected_buckets_from_mapping(df_map, "RawBucket")
        buckets_curated = _selected_buckets_from_mapping(df_map, "CuratedBucket")

        st.write(
            f"**Selected Landing buckets:** {len(buckets_landing)}  |  "
            f"**Raw:** {len(buckets_raw)}  |  "
            f"**Curated:** {len(buckets_curated)}"
        )

    st.markdown("---")

    # Action bar: list per type + latest + clear
    a1, a2, a3, a4, a5 = st.columns([1, 1, 1, 1, 1])
    list_landing_btn = a1.button("List Landing Buckets", type="primary", use_container_width=True, key="flow_list_landing")
    list_raw_btn = a2.button("List Raw Buckets", use_container_width=True, key="flow_list_raw")
    list_curated_btn = a3.button("List Curated Buckets", use_container_width=True, key="flow_list_curated")

    latest_btn = a4.button("Get Latest per Bucket", use_container_width=True, disabled=versions_mode, key="flow_latest")
    clear_btn = a5.button("Clear Results", use_container_width=True, key="flow_clear")

    if clear_btn:
        st.session_state.pop(SK.FLOW_S3_RESULTS, None)
        # clear per-bucket selection row indexes
        for k in list(st.session_state.keys()):
            if k.startswith("flow_selrows_") or k.startswith("flow_del_confirm_"):
                st.session_state.pop(k, None)
        st.success("Cleared listed results (filters retained).")
        st.rerun()

    def _list_for_buckets(buckets: List[str], label: str):
        if not buckets:
            st.warning(f"No {label} buckets selected.")
            return

        results: Dict[str, pd.DataFrame] = {}
        with st.status(f"Listing objects across {len(buckets)} {label} bucket(s)...", expanded=True) as status:
            for bucket in buckets:
                try:
                    if versions_mode:
                        rows = browser.list_object_versions(
                            bucket=bucket,
                            prefix=prefix_all or "",
                            cap=max_items,
                            start_utc=start_utc,
                            end_utc=end_utc,
                            include_delete_markers=include_delete_markers
                        )
                    else:
                        rows = browser.list_objects(
                            bucket=bucket,
                            prefix=prefix_all or "",
                            cap=max_items,
                            start_utc=start_utc,
                            end_utc=end_utc
                        )

                    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["S3 URI", "Key", "Size (MB)", "LastModified"])
                    results[bucket] = df.reset_index(drop=True)
                    st.write(f"✔️ {bucket}: {len(df)} rows")
                except Exception as e:
                    st.write(f"❌ {bucket}: {e}")
                    results[bucket] = pd.DataFrame(columns=["S3 URI", "Key", "Size (MB)", "LastModified"])

            st.session_state[SK.FLOW_S3_RESULTS] = {
                "bucket_type": label,
                "buckets": buckets,
                "prefix": prefix_all or "",
                "versions": versions_mode,
                "delmarkers": include_delete_markers,
                "time_enabled": enable_time_filter,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "max_items": max_items,
                "tables": results,
            }
            status.update(label="Listing complete", state="complete", expanded=False)

    if list_landing_btn:
        _list_for_buckets(buckets_landing, "Landing")

    if list_raw_btn:
        _list_for_buckets(buckets_raw, "Raw")

    if list_curated_btn:
        _list_for_buckets(buckets_curated, "Curated")

    # Render listing results + 1-click selection + download/delete
    if SK.FLOW_S3_RESULTS in st.session_state:
        payload = st.session_state[SK.FLOW_S3_RESULTS]
        tables: Dict[str, pd.DataFrame] = payload.get("tables", {}) or {}
        bucket_type = payload.get("bucket_type", "Buckets")

        if enable_time_filter:
            start_syd = start_utc.astimezone(SYDNEY_TZ) if start_utc else None
            end_syd = end_utc.astimezone(SYDNEY_TZ) if end_utc else None
            st.info(f"Filtered LastModified between **{start_syd}** and **{end_syd}** (Sydney time).")

        st.markdown(f"### Results — {_fmt_bucket_type_label(bucket_type)} Buckets")
        st.caption(f"Prefix: `{prefix_all or ''}` | Versions: `{versions_mode}` | Delete markers: `{include_delete_markers}`")

        dest_dir = Path(os.path.expanduser(st.session_state.get(SK.FLOW_DOWNLOAD_DIR, ""))).resolve()
        preserve_structure = bool(st.session_state.get(SK.FLOW_PRESERVE, True))
        for bucket, df in tables.items():
            st.markdown(f"#### 🪣 {bucket} — {len(df) if df is not None else 0} item(s)")

            if df is None or df.empty:
                st.write("_No objects found for current filters._")
                st.markdown("---")
                continue

            # Show a useful view
            display_cols = []
            for c in ["S3 URI", "Key", "VersionId", "IsDeleteMarker", "Size (MB)", "LastModified", "StorageClass"]:
                if c in df.columns:
                    display_cols.append(c)
            view_df = df[display_cols].copy().reset_index(drop=True)

            # Single-click fix: restore previous selection via `default`
            _sel_key = f"flow_selrows_{bucket}"
            _prev_sel: List[int] = st.session_state.get(_sel_key, [])

            evt = st.dataframe(
                view_df,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key=f"flow_tbl_{bucket}",
            )
            sel_rows = list(evt.selection.rows or []) if evt and getattr(evt, "selection", None) else _prev_sel
            st.session_state[_sel_key] = sel_rows

            selected_df = view_df.iloc[sel_rows].copy() if sel_rows else view_df.iloc[0:0].copy()
            st.write(f"Selected: **{len(selected_df)}** item(s).")
            st.caption(f"Download destination: `{dest_dir}`")

            # Build items for download/delete from selection
            def _build_items_for_ops() -> Tuple[List[Dict], List[str], bool]:
                items: List[Dict] = []
                keys: List[str] = []
                has_versions = "VersionId" in view_df.columns and versions_mode
                for _, row in selected_df.iterrows():
                    key = row.get("Key")
                    if not key:
                        continue
                    if versions_mode:
                        if row.get("IsDeleteMarker"):
                            if not include_delete_markers:
                                continue
                        vid = row.get("VersionId")
                        if pd.notna(vid) and vid is not None:
                            items.append({"Key": key, "VersionId": vid})
                    else:
                        keys.append(key)
                        items.append({"Key": key})
                return items, keys, has_versions

            items, keys, has_versions = _build_items_for_ops()

            # Buttons row
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                dl = st.button(
                    f"⬇️ Download Selected",
                    type="primary",
                    use_container_width=True,
                    disabled=len(items) == 0,
                    key=f"flow_dl_{bucket}"
                )
            with c2:
                conf_key = f"flow_del_confirm_{bucket}"
                conf = st.text_input(
                    "Type DELETE to enable deletion",
                    value=st.session_state.get(conf_key, ""),
                    key=conf_key,
                    placeholder="DELETE",
                    label_visibility="collapsed",
                )
                del_enabled = (conf or "").strip().upper() == "DELETE" and len(items) > 0
                de = st.button(
                    "🗑️ Delete Selected",
                    use_container_width=True,
                    disabled=not del_enabled,
                    key=f"flow_del_{bucket}"
                )
            with c3:
                st.caption("Tip: Select rows with one click. Use Shift/Ctrl for multi-select.")

            # keep your existing dl/de handlers unchanged below...
            # (your current `if dl:` and `if de:` blocks stay the same)

            st.markdown("---")

            if dl:
                with st.status(f"Downloading {len(items)} file(s) from {bucket} ...", expanded=True) as status:
                    saved, failed = downloader.download_many(
                        bucket=bucket,
                        items=items,
                        dest_dir=dest_dir,
                        base_prefix=prefix_all or "",
                        preserve_structure=preserve_structure
                    )
                    if saved:
                        st.success(f"Saved {len(saved)} file(s):")
                        for p in saved[:50]:
                            st.write(f"- `{p}`")
                        if len(saved) > 50:
                            st.caption(f"...and {len(saved) - 50} more")
                    if failed:
                        st.error(f"{len(failed)} file(s) failed:")
                        for key, err in failed[:20]:
                            st.write(f"- {key} → {err}")
                        if len(failed) > 20:
                            st.caption(f"...and {len(failed) - 20} more")
                    status.update(label="Download complete", state="complete", expanded=False)

            if de:
                with st.status(f"Deleting {len(items)} item(s) from {bucket} ...", expanded=True) as status:
                    if versions_mode:
                        deleted_count, errors = deleter.delete_versions(bucket=bucket, items=items, batch_size=1000)
                    else:
                        deleted_count, errors = deleter.delete_current(bucket=bucket, keys=keys, batch_size=1000)

                    st.success(f"Deleted {deleted_count} item(s).")
                    if errors:
                        st.error(f"{len(errors)} error(s) occurred:")
                        for e in errors[:20]:
                            if "VersionId" in e:
                                st.write(f"- {e.get('Key')} (v={e.get('VersionId')}) → {e.get('Code')}: {e.get('Message')}")
                            else:
                                st.write(f"- {e.get('Key')} → {e.get('Code')}: {e.get('Message')}")
                        if len(errors) > 20:
                            st.caption(f"...and {len(errors) - 20} more")
                    status.update(label="Deletion complete", state="complete", expanded=False)

    # Latest per bucket (only in current objects mode, like original)
    if latest_btn:
        if versions_mode:
            st.warning("Latest lookup disabled while in 'versions' mode.")
        else:
            # determine buckets from last listing if exists; else from mapping
            buckets_for_latest = []
            if SK.FLOW_S3_RESULTS in st.session_state:
                buckets_for_latest = st.session_state[SK.FLOW_S3_RESULTS].get("buckets", []) or []
            if not buckets_for_latest:
                # fall back
                buckets_for_latest = buckets_raw or buckets_landing or buckets_curated

            if not buckets_for_latest:
                st.warning("No buckets available for latest lookup.")
            else:
                with st.status("Finding latest object per bucket...", expanded=True) as status:
                    for bucket in buckets_for_latest:
                        latest = browser.find_latest_object(bucket, prefix_all or "", start_utc=start_utc, end_utc=end_utc)
                        if not latest:
                            st.write(f"- {bucket}: No objects found.")
                        else:
                            st.write(f"- {bucket}: Latest object")
                            st.json({
                                "S3 URI": latest["S3 URI"],
                                "Key": latest["Key"],
                                "Size (MB)": latest["Size (MB)"],
                                "LastModified (UTC)": latest["LastModified"].isoformat() if latest["LastModified"] else None,
                                "StorageClass": latest.get("StorageClass", "STANDARD"),
                            })
                    status.update(label="Lookup complete", state="complete", expanded=False)

# ──────────────────────────────────────────────────────────────────────────────
# TAB 2: ETL QA Tools (reusing shared filters as defaults)
# ──────────────────────────────────────────────────────────────────────────────
with tab_qa:
    st.subheader("🧪 ETL QA Tools")

    # Mapping received from App Discovery (reuse same df used in Analyse)
    st.markdown("#### 🗂️ Apps ↔ Buckets Mapping")
    mapping_df = _get_mapping_df()
    if mapping_df is None:
        st.info("No mapping received. Go to **App Discovery → Test S3** (or map buckets) to send mapping here.")
    else:
        df_map = _ensure_mapping_select_col(mapping_df)
        nrows = len(df_map)
        sel_rows = st.session_state.get(SK.QA_MAP_SELECTED, list(range(nrows))) or list(range(nrows))

        mh1, mh2, mh3 = st.columns([1, 1, 1])
        with mh1:
            if st.button("✅ Select All (Mapping)", use_container_width=True, key="qa_map_sel_all_btn"):
                st.session_state[SK.QA_MAP_SELECTED] = list(range(nrows))
                st.rerun()
        with mh2:
            if st.button("🧹 Clear (Mapping)", use_container_width=True, key="qa_map_clear_btn"):
                st.session_state[SK.QA_MAP_SELECTED] = []
                st.rerun()
        with mh3:
            if st.button("🔁 Invert (Mapping)", use_container_width=True, key="qa_map_invert_btn"):
                st.session_state[SK.QA_MAP_SELECTED] = sorted(set(range(nrows)) - set(sel_rows))
                st.rerun()

        view_map = df_map[["EnterpriseAppID", "LandingBucket", "RawBucket", "CuratedBucket"]].copy()

        # Single-click fix
        evt = st.dataframe(
            view_map,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key="qa_map_df",
        )
        chosen = list(evt.selection.rows or []) if evt and getattr(evt, "selection", None) else sel_rows
        st.session_state[SK.QA_MAP_SELECTED] = chosen

        df_map["Select"] = False
        if chosen:
            df_map.loc[chosen, "Select"] = True

        # persist
        st.session_state[SK.QA_MAPPING_DF] = df_map
    # ──────────────────────────────────────────────────────────────────────────
        # Entity Path Helpers (RAW & CURATED)
        # Lists entity paths so you can copy/paste OR send directly into Manual explorer
        # ──────────────────────────────────────────────────────────────────────────
        st.markdown("#### 🧭 Entity Path Helpers")

        c_ent1, c_ent2, c_ent3 = st.columns([1, 1, 2])
        btn_list_raw_entities = c_ent1.button(
            "📜 List RAW entity paths",
            use_container_width=True,
            key="qa_btn_list_raw_entities"
        )
        btn_list_cur_entities = c_ent2.button(
            "📜 List CURATED entity paths",
            use_container_width=True,
            key="qa_btn_list_cur_entities"
        )
        btn_clear_entities = c_ent3.button(
            "🧹 Clear entity paths",
            use_container_width=True,
            key="qa_btn_clear_entity_paths"
        )

        if btn_clear_entities:
            st.session_state.pop(SK.QA_RAW_ENTITY_PATHS_DF, None)
            st.session_state.pop(SK.QA_CURATED_ENTITY_PATHS_DF, None)
            st.session_state.pop("_ep_raw_sel_row", None)
            st.session_state.pop("_ep_cur_sel_row", None)
            st.success("Cleared entity paths.")
            st.rerun()

        if btn_list_raw_entities:
            df_raw_ep = _build_entity_paths_df("raw")
            if df_raw_ep is not None:
                st.session_state[SK.QA_RAW_ENTITY_PATHS_DF] = df_raw_ep

        if btn_list_cur_entities:
            df_cur_ep = _build_entity_paths_df("curated")
            if df_cur_ep is not None:
                st.session_state[SK.QA_CURATED_ENTITY_PATHS_DF] = df_cur_ep

        # Show RAW entity paths table
        if SK.QA_RAW_ENTITY_PATHS_DF in st.session_state:
            df_raw_paths = st.session_state[SK.QA_RAW_ENTITY_PATHS_DF]
            with st.expander(f"📄 RAW entity paths ({len(df_raw_paths)} rows) — click a row → 'Use selected' to fill Manual S3 Path", expanded=True):
                _ep_raw_sel = st.session_state.get("_ep_raw_sel_row", [])
                evt_raw_paths = st.dataframe(
                    df_raw_paths,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="qa_raw_entity_paths_table",
                )
                chosen_raw_idx: List[int] = list(evt_raw_paths.selection.rows or []) if evt_raw_paths and getattr(evt_raw_paths, "selection", None) else _ep_raw_sel
                st.session_state["_ep_raw_sel_row"] = chosen_raw_idx

                chosen_raw_path = None
                if chosen_raw_idx:
                    chosen_raw_path = df_raw_paths.iloc[chosen_raw_idx[0]]["S3 Path"]
                    st.caption(f"Selected path: `{chosen_raw_path}`")
                use_raw_btn = st.button("📋 Use selected (RAW)", key="qa_use_raw_path_btn", disabled=not chosen_raw_path)
                if use_raw_btn and chosen_raw_path:
                    st.session_state[SK.QA_S3_PATH] = chosen_raw_path
                    st.session_state[SK.S3_PATH] = chosen_raw_path
                    st.success("Path copied to the 'S3 path' field in Manual Explorer. You can now click 'Scan Path' or 'Top 10 Rows'.")
                    st.rerun()

        # Show CURATED entity paths table
        if SK.QA_CURATED_ENTITY_PATHS_DF in st.session_state:
            df_cur_paths = st.session_state[SK.QA_CURATED_ENTITY_PATHS_DF]
            with st.expander(f"🧬 CURATED entity paths ({len(df_cur_paths)} rows) — click a row → 'Use selected' to fill Manual S3 Path", expanded=True):
                _ep_cur_sel = st.session_state.get("_ep_cur_sel_row", [])
                evt_cur_paths = st.dataframe(
                    df_cur_paths,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="qa_cur_entity_paths_table",
                )
                chosen_cur_idx: List[int] = list(evt_cur_paths.selection.rows or []) if evt_cur_paths and getattr(evt_cur_paths, "selection", None) else _ep_cur_sel
                st.session_state["_ep_cur_sel_row"] = chosen_cur_idx

                chosen_cur_path = None
                if chosen_cur_idx:
                    chosen_cur_path = df_cur_paths.iloc[chosen_cur_idx[0]]["S3 Path"]
                    st.caption(f"Selected path: `{chosen_cur_path}`")
                use_cur_btn = st.button("📋 Use selected (CURATED)", key="qa_use_cur_path_btn", disabled=not chosen_cur_path)
                if use_cur_btn and chosen_cur_path:
                    st.session_state[SK.QA_S3_PATH] = chosen_cur_path
                    st.session_state[SK.S3_PATH] = chosen_cur_path
                    st.success("Path copied to the 'S3 path' field in Manual Explorer. You can now click 'Scan Path' or 'Top 10 Rows'.")
                    st.rerun()
    st.divider()

    # Use shared filters for QA checks
    st.markdown("### 🎚️ QA Filters (from Shared Sidebar)")
    st.write(
        f"- Versions: **{versions_mode}**  | Delete markers: **{include_delete_markers}**  | "
        f"Time filter: **{enable_time_filter}**  | Cap per prefix: **{cap_per_prefix}**"
    )
    if enable_time_filter:
        start_syd = start_utc.astimezone(SYDNEY_TZ) if start_utc else None
        end_syd = end_utc.astimezone(SYDNEY_TZ) if end_utc else None
        st.caption(f"Time range (Sydney): {start_syd} → {end_syd}")

    st.divider()

    # Inner QA tabs
    _QA_TAB_LABELS = ["✅ Automated Checks", "🧭 Manual Explorer"]

    tab_checks, tab_manual = st.tabs(_QA_TAB_LABELS)

    with tab_checks:
        _has_raw_ep = SK.QA_RAW_ENTITY_PATHS_DF in st.session_state
        _has_cur_ep = SK.QA_CURATED_ENTITY_PATHS_DF in st.session_state
        _has_any_ep = _has_raw_ep or _has_cur_ep

        if not _has_any_ep:
            st.info("List RAW and/or CURATED entity paths above to enable automated checks.")
        else:
            st.markdown("#### Actions")
            c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

            btn_lastfile = c1.button("⏱️ Fetch Last File Date/Time", type="primary", use_container_width=True, key="qa_btn_lastfile")
            btn_rawtypes = c2.button(f"📄 Raw file extension check (last {RAW_LAST_N_DATES} dates)", use_container_width=True, key="qa_btn_rawtypes", disabled=not _has_raw_ep)
            btn_curatedschema = c3.button(f"🧬 Test curated schema changes (last {CURATED_LAST_N_BATCHES} batches)", use_container_width=True, key="qa_btn_curatedschema", disabled=not _has_cur_ep)
            btn_clear_tests = c4.button("🧹 Clear Test Results", use_container_width=True, key="qa_btn_clear_tests")

            if btn_clear_tests:
                # Remove result columns from entity path tables
                for _sk in (SK.QA_RAW_ENTITY_PATHS_DF, SK.QA_CURATED_ENTITY_PATHS_DF):
                    if _sk in st.session_state and isinstance(st.session_state[_sk], pd.DataFrame):
                        _df_c = st.session_state[_sk]
                        for _col in ["LastFileDateTime", "ExtCheck", "ExtDetail", "SchemaCheck", "SchemaDetail"]:
                            if _col in _df_c.columns:
                                _df_c = _df_c.drop(columns=[_col])
                        st.session_state[_sk] = _df_c
                st.success("Cleared test result columns from entity tables.")
                st.rerun()

            # ── Fetch Last File Date/Time ──────────────────────────────────────────
            if btn_lastfile:
                with st.status("Fetching latest file timestamps…", expanded=True):
                    # Update RAW entity paths table — use S3 Path column to find true latest
                    if _has_raw_ep:
                        df_raw_ep = st.session_state[SK.QA_RAW_ENTITY_PATHS_DF].copy()
                        last_times_raw = []
                        for _, r in df_raw_ep.iterrows():
                            s3_path_val = (r.get("S3 Path") or "").strip()
                            if not s3_path_val:
                                last_times_raw.append(None)
                                continue
                            try:
                                bkt, pref = S3Utils.parse_s3_path(s3_path_val)
                            except ValueError:
                                last_times_raw.append(None)
                                continue
                            latest_obj = browser.find_latest_object(bkt, pref, start_utc=start_utc, end_utc=end_utc)
                            if latest_obj and latest_obj.get("LastModified"):
                                lt_syd = latest_obj["LastModified"].astimezone(SYDNEY_TZ)
                                last_times_raw.append(lt_syd.strftime("%Y-%m-%d %H:%M:%S %Z"))
                            else:
                                last_times_raw.append(None)
                        df_raw_ep["LastFileDateTime"] = last_times_raw
                        st.session_state[SK.QA_RAW_ENTITY_PATHS_DF] = df_raw_ep
                        st.write(f"✔️ Updated {len(df_raw_ep)} RAW entities")

                    # Update CURATED entity paths table — use S3 Path column to find true latest
                    if _has_cur_ep:
                        df_cur_ep = st.session_state[SK.QA_CURATED_ENTITY_PATHS_DF].copy()
                        last_times_cur = []
                        for _, r in df_cur_ep.iterrows():
                            s3_path_val = (r.get("S3 Path") or "").strip()
                            if not s3_path_val:
                                last_times_cur.append(None)
                                continue
                            try:
                                bkt, pref = S3Utils.parse_s3_path(s3_path_val)
                            except ValueError:
                                last_times_cur.append(None)
                                continue
                            latest_obj = browser.find_latest_object(bkt, pref, start_utc=start_utc, end_utc=end_utc)
                            if latest_obj and latest_obj.get("LastModified"):
                                lt_syd = latest_obj["LastModified"].astimezone(SYDNEY_TZ)
                                last_times_cur.append(lt_syd.strftime("%Y-%m-%d %H:%M:%S %Z"))
                            else:
                                last_times_cur.append(None)
                        df_cur_ep["LastFileDateTime"] = last_times_cur
                        st.session_state[SK.QA_CURATED_ENTITY_PATHS_DF] = df_cur_ep
                        st.write(f"✔️ Updated {len(df_cur_ep)} CURATED entities")
                st.rerun()

            # ── Raw file extension check ───────────────────────────────────────────
            if btn_rawtypes:
                df_raw_ep = st.session_state[SK.QA_RAW_ENTITY_PATHS_DF].copy()
                ext_statuses: List[str] = []
                ext_details: List[str] = []
                with st.status("Running raw file extension check…", expanded=False):
                    for _, r in df_raw_ep.iterrows():
                        rb = (r.get("Bucket") or "").strip()
                        ent = (r.get("Entity") or "").strip()
                        if not rb or not ent:
                            ext_statuses.append("SKIP")
                            ext_details.append("Missing bucket or entity")
                            continue

                        if ent == "default":
                            pref = "entity/default/"
                            if versions_mode:
                                items = browser.list_object_versions(
                                    bucket=rb, prefix=pref, cap=cap_per_prefix,
                                    start_utc=start_utc, end_utc=end_utc,
                                    include_delete_markers=include_delete_markers
                                )
                                items = [it for it in items if not it.get("IsDeleteMarker")]
                            else:
                                items = browser.list_objects(bucket=rb, prefix=pref, cap=cap_per_prefix, start_utc=start_utc, end_utc=end_utc)

                            types_here = sorted({extract_file_extension(it.get("Key", "")) for it in items if it.get("Key")})
                            status = "PASS" if len(types_here) <= 1 else "WARN"
                            detail = f"default folder types: {types_here or ['(none)']}"
                        else:
                            base = f"entity/{ent}/"
                            last_dates = _find_last_n_dates_with_data(
                                rb, base, RAW_LAST_N_DATES, versions_mode, include_delete_markers, start_utc, end_utc, cap_per_prefix
                            )
                            if len(last_dates) < RAW_LAST_N_DATES:
                                status = "WARN"
                                detail = f"Found {len(last_dates)} date(s) in range: {last_dates}"
                            else:
                                types_per_date = []
                                for d in last_dates:
                                    pref = f"{base}{d}/"
                                    if versions_mode:
                                        items = browser.list_object_versions(
                                            bucket=rb, prefix=pref, cap=cap_per_prefix,
                                            start_utc=start_utc, end_utc=end_utc,
                                            include_delete_markers=include_delete_markers
                                        )
                                        items = [it for it in items if not it.get("IsDeleteMarker")]
                                    else:
                                        items = browser.list_objects(bucket=rb, prefix=pref, cap=cap_per_prefix, start_utc=start_utc, end_utc=end_utc)

                                    exts = sorted({extract_file_extension(it.get("Key", "")) for it in items if it.get("Key")})
                                    types_per_date.append(exts)

                                if types_per_date:
                                    stable = len({",".join(x) for x in types_per_date}) == 1
                                    status = "PASS" if (stable and len(types_per_date[0]) == 1) else ("WARN" if stable else "FAIL")
                                    detail = f"Dates={last_dates}; Types={types_per_date}"
                                else:
                                    status = "WARN"
                                    detail = "No files found"

                        ext_statuses.append(status)
                        ext_details.append(detail)

                df_raw_ep["ExtCheck"] = ext_statuses
                df_raw_ep["ExtDetail"] = ext_details
                st.session_state[SK.QA_RAW_ENTITY_PATHS_DF] = df_raw_ep
                st.rerun()

            # ── Test curated schema changes ────────────────────────────────────────
            if btn_curatedschema:
                df_cur_ep = st.session_state[SK.QA_CURATED_ENTITY_PATHS_DF].copy()
                schema_statuses: List[str] = []
                schema_details: List[str] = []
                with st.status("Testing curated schema across last batches…", expanded=False):
                    for _, r in df_cur_ep.iterrows():
                        cb = (r.get("Bucket") or "").strip()
                        ent = (r.get("Entity") or "").strip()
                        if not cb or not ent:
                            schema_statuses.append("SKIP")
                            schema_details.append("Missing bucket or entity")
                            continue

                        batches = _find_last_n_batches_with_data(
                            cb, ent, CURATED_LAST_N_BATCHES, versions_mode, include_delete_markers, start_utc, end_utc, cap_per_prefix
                        )
                        if not batches:
                            schema_statuses.append("WARN")
                            schema_details.append("No batches with data in range")
                            continue

                        schemas = []
                        sampled_info = []
                        for b in batches:
                            pref = f"{ent}/{b}/"
                            row = _sample_row_in_prefix(cb, pref, versions_mode, include_delete_markers, start_utc, end_utc, cap_per_prefix)
                            if not row:
                                schemas.append(set())
                                sampled_info.append(None)
                                continue

                            key = row.get("Key")
                            version_id = row.get("VersionId") if versions_mode else None
                            ftype = QAInspector.guess_type(key)
                            try:
                                cols = qa.list_columns(
                                    bucket=cb, key=key, ftype=ftype,
                                    version_id=version_id if version_id else None
                                )
                                schemas.append(set(cols or []))
                                sampled_info.append(f"{key}{f' (v={version_id})' if version_id else ''}")
                            except Exception as e:
                                schemas.append(set())
                                sampled_info.append(f"{key} (err: {e})")

                        base_schema = schemas[0] if schemas else set()
                        changed = any(s != base_schema for s in schemas[1:])

                        if changed:
                            union_all = set().union(*schemas) if schemas else set()
                            added_columns = sorted(union_all - base_schema)
                            removed_columns = sorted(base_schema - union_all)
                            detail = f"Batches={batches}; Sampled={sampled_info}; Changes → added={added_columns}, removed={removed_columns}"
                            status = "FAIL"
                        else:
                            detail = f"Batches={batches}; Sampled={sampled_info}; No column changes"
                            status = "PASS"

                        schema_statuses.append(status)
                        schema_details.append(detail)

                df_cur_ep["SchemaCheck"] = schema_statuses
                df_cur_ep["SchemaDetail"] = schema_details
                st.session_state[SK.QA_CURATED_ENTITY_PATHS_DF] = df_cur_ep
                st.rerun()

    with tab_manual:
        st.markdown("#### S3 Dataset / File")

        # Default path comes from App Discovery "Test S3" or user's last input
        s3_path = st.text_input(
            "S3 path (file OR folder)",
            value=st.session_state.get(SK.QA_S3_PATH, st.session_state.get(SK.S3_PATH, "")),
            key="flow_qa_manual_s3_path",
            placeholder="e.g., s3://bucket/folder/ or s3://bucket/file.parquet"
        )
        st.session_state[SK.QA_S3_PATH] = s3_path

        # Manual explorer controls (still present; uses shared filters as default)
        mc1, mc2, mc3 = st.columns([1, 1.6, 1.4])

        with mc1:
            m_versions = st.checkbox(
                "List versions",
                value=st.session_state.get("qa_m_versions", versions_mode),
                key="flow_qa_m_versions"
            )
            st.session_state["qa_m_versions"] = m_versions

            m_show_dm = st.checkbox(
                "Include delete markers",
                value=st.session_state.get("qa_m_dm", False),
                disabled=not m_versions,
                key="flow_qa_m_dm"
            )
            st.session_state["qa_m_dm"] = m_show_dm

        with mc2:
            m_time = st.checkbox(
                "Enable time range",
                value=st.session_state.get("qa_m_time", enable_time_filter),
                key="flow_qa_m_time"
            )
            st.session_state["qa_m_time"] = m_time

            default_start, default_end = get_default_date_range()
            m_start = st.datetime_input(
                "Start",
                value=st.session_state.get("qa_m_start", start_dt if enable_time_filter else default_start),
                disabled=not m_time,
                key="flow_qa_m_start"
            )
            m_end = st.datetime_input(
                "End",
                value=st.session_state.get("qa_m_end", end_dt if enable_time_filter else default_end),
                disabled=not m_time,
                key="flow_qa_m_end"
            )
            st.session_state["qa_m_start"] = m_start
            st.session_state["qa_m_end"] = m_end

        with mc3:
            m_cap = st.number_input(
                "Max files to scan",
                min_value=1, max_value=5000, step=50,
                value=int(st.session_state.get("qa_m_cap", 500)),
                key="flow_qa_m_cap"
            )
            st.session_state["qa_m_cap"] = int(m_cap)

        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1])
        scan_btn = c1.button("🔍 Scan Path", type="primary", use_container_width=True, key="flow_qa_scan_btn")
        preview_btn = c2.button("👀 Top 10 Rows", use_container_width=True, key="flow_qa_preview_btn")
        cols_btn = c3.button("🧱 List Columns", use_container_width=True, key="flow_qa_cols_btn")
        count_btn = c4.button("🔢 Row Count", use_container_width=True, key="flow_qa_count_btn")
        clear_btn = c5.button("🧹 Clear", use_container_width=True, key="flow_qa_clear_btn")

        if clear_btn:
            for k in ["qa_scan_df", "qa_selected_df"]:
                st.session_state.pop(k, None)
            st.rerun()

        if scan_btn:
            try:
                bucket, prefix = S3Utils.parse_s3_path(s3_path)
            except ValueError as e:
                st.error(str(e))
                st.stop()

            m_start_utc = S3Utils.to_utc(m_start) if m_time else None
            m_end_utc = S3Utils.to_utc(m_end) if m_time else None

            is_file = False
            if prefix and not prefix.endswith("/"):
                try:
                    s3.head_object(Bucket=bucket, Key=prefix)
                    is_file = True
                except Exception:
                    is_file = False

            with st.status(f"Scanning s3://{bucket}/{prefix}", expanded=False):
                try:
                    rows = []
                    if is_file and not m_versions:
                        obj = s3.head_object(Bucket=bucket, Key=prefix)
                        key = prefix
                        lm = obj["LastModified"]
                        if lm and lm.tzinfo is None:
                            lm = lm.replace(tzinfo=timezone.utc)
                        ftype = QAInspector.guess_type(key)
                        rows.append({
                            "S3 URI": S3Utils.build_s3_uri(bucket, key),
                            "Key": key,
                            "Type": ftype,
                            "VersionId": None,
                            "IsDeleteMarker": False,
                            "Size (MB)": round((obj.get("ContentLength", 0) or 0) / (1024 * 1024), 3),
                            "LastModified": lm,
                        })
                    else:
                        if m_versions:
                            items = browser.list_object_versions(
                                bucket=bucket,
                                prefix=prefix,
                                cap=m_cap,
                                start_utc=m_start_utc,
                                end_utc=m_end_utc,
                                include_delete_markers=m_show_dm
                            )
                            for it in items:
                                key = it["Key"]
                                ftype = QAInspector.guess_type(key)
                                rows.append({
                                    "S3 URI": it["S3 URI"],
                                    "Key": key,
                                    "Type": ftype,
                                    "VersionId": it.get("VersionId"),
                                    "IsDeleteMarker": it.get("IsDeleteMarker", False),
                                    "Size (MB)": it.get("Size (MB)"),
                                    "LastModified": it.get("LastModified"),
                                })
                        else:
                            items = browser.list_objects(
                                bucket=bucket,
                                prefix=prefix,
                                cap=m_cap,
                                start_utc=m_start_utc,
                                end_utc=m_end_utc
                            )
                            for it in items:
                                key = it["Key"]
                                ftype = QAInspector.guess_type(key)
                                rows.append({
                                    "S3 URI": it["S3 URI"],
                                    "Key": key,
                                    "Type": ftype,
                                    "VersionId": None,
                                    "IsDeleteMarker": False,
                                    "Size (MB)": it.get("Size (MB)"),
                                    "LastModified": it.get("LastModified"),
                                })

                    if not rows:
                        st.info("No files found for current filters.")
                        st.session_state.pop(SK.QA_SCAN_DF, None)
                    else:
                        df = pd.DataFrame(rows).reset_index(drop=True)
                        st.session_state[SK.QA_SCAN_DF] = df
                        st.session_state[SK.QA_SCAN_SEL_ROWS] = []
                        st.success(f"Scanned {len(df)} item(s).")

                except Exception as e:
                    st.error(f"Unexpected error: {e}")

        # Single-click selection for scanned files
        if SK.QA_SCAN_DF in st.session_state:
            st.markdown("#### Files (click rows to select)")
            df_scan = st.session_state[SK.QA_SCAN_DF].copy().reset_index(drop=True)

            _scan_prev = st.session_state.get(SK.QA_SCAN_SEL_ROWS, [])
            evt = st.dataframe(
                df_scan,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key="flow_qa_scan_df",
            )
            sel_rows = list(evt.selection.rows or []) if evt and getattr(evt, "selection", None) else _scan_prev
            st.session_state[SK.QA_SCAN_SEL_ROWS] = sel_rows

            selected = df_scan.iloc[sel_rows].copy() if sel_rows else df_scan.iloc[0:0].copy()
            st.session_state[SK.QA_SELECTED_DF] = selected
            st.write(f"Selected **{len(selected)}** item(s).")

        def _first_selected_row():
            if SK.QA_SELECTED_DF not in st.session_state or len(st.session_state[SK.QA_SELECTED_DF]) == 0:
                st.error("Select at least one file (click a row).")
                return None
            if len(st.session_state[SK.QA_SELECTED_DF]) > 1:
                st.info("Previewing the first selected file only.")
            return st.session_state[SK.QA_SELECTED_DF].iloc[0]

        if preview_btn:
            row = _first_selected_row()
            if row is not None:
                path = row["S3 URI"]
                key = row["Key"]
                ftype = row["Type"]
                version_id = row.get("VersionId")
                if row.get("IsDeleteMarker", False):
                    st.warning("The selected version is a delete marker. Pick a different version.")
                else:
                    bucket, _ = S3Utils.parse_s3_path(path)
                    with st.status(f"Loading head(10) from {path} as {ftype} ...", expanded=False):
                        try:
                            df_head = qa.preview_head(bucket=bucket, key=key, ftype=ftype, n=10,
                                                     version_id=version_id if pd.notna(version_id) else None)
                            if df_head.empty:
                                st.info("No rows to preview.")
                            else:
                                st.dataframe(df_head, use_container_width=True)
                            st.success("Preview complete")
                        except Exception as e:
                            st.error(f"Preview failed: {e}")

        if cols_btn:
            row = _first_selected_row()
            if row is not None:
                path = row["S3 URI"]
                key = row["Key"]
                ftype = row["Type"]
                version_id = row.get("VersionId")
                if row.get("IsDeleteMarker", False):
                    st.warning("The selected version is a delete marker. Pick a different version.")
                else:
                    bucket, _ = S3Utils.parse_s3_path(path)
                    with st.status(f"Fetching columns from {path} as {ftype} ...", expanded=False):
                        try:
                            cols = qa.list_columns(bucket=bucket, key=key, ftype=ftype,
                                                  version_id=version_id if pd.notna(version_id) else None)
                            if not cols:
                                st.info("No columns detected.")
                            else:
                                st.write(f"**Columns ({len(cols)}):**")
                                st.code(", ".join(cols), language="text")
                            st.success("Columns fetched")
                        except Exception as e:
                            st.error(f"Column detection failed: {e}")

        if count_btn:
            if SK.QA_SELECTED_DF not in st.session_state or len(st.session_state[SK.QA_SELECTED_DF]) == 0:
                st.error("Select at least one file (click a row).")
            else:
                sel = st.session_state[SK.QA_SELECTED_DF]
                total = 0
                results = []
                with st.status(f"Counting rows for {len(sel)} file(s)...", expanded=False):
                    try:
                        for _, r in sel.iterrows():
                            if r.get("IsDeleteMarker", False):
                                results.append({"S3 URI": r["S3 URI"], "Type": r["Type"], "RowCount": None, "Error": "Delete marker"})
                                continue
                            key = r["Key"]
                            ftype = r["Type"]
                            version_id = r.get("VersionId")
                            try:
                                bucket, _ = S3Utils.parse_s3_path(r["S3 URI"])
                                cnt = qa.rowcount(bucket=bucket, key=key, ftype=ftype,
                                                 version_id=version_id if pd.notna(version_id) else None)
                                total += (cnt or 0)
                                results.append({"S3 URI": r["S3 URI"], "Type": ftype, "RowCount": cnt})
                            except Exception as e:
                                results.append({"S3 URI": r["S3 URI"], "Type": ftype, "RowCount": None, "Error": str(e)})

                        dfc = pd.DataFrame(results)
                        st.dataframe(dfc, use_container_width=True)
                        st.success(f"Grand Total Rows: **{total:,}**")
                    except Exception as e:
                        st.error(f"Counting failed: {e}")

st.markdown("---")
st.caption("← Go back to **App Discovery** to adjust apps or mappings.")