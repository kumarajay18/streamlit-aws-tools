# sql/registry.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import streamlit as st

# Base directory for .sql files (this file sits inside /sql)
SQL_DIR = Path(__file__).parent


@st.cache_data(show_spinner=False)
def load_sql(file_name: str) -> str:
    """
    Load the contents of a .sql file from the sql/ directory.

    Results are cached by Streamlit so the file is read only once per server
    run (or whenever the file content changes).

    Raises:
        FileNotFoundError: When the requested ``.sql`` file does not exist.
    """
    path = SQL_DIR / file_name
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# QUERY_REGISTRY
# ---------------------------------------------------------------------------
# Structure:  query_display_name -> {
#     "category": str,         — used for grouping in the UI
#     "file":     str,         — filename inside sql/
#     "params":   List[str],   — positional bind variables for the SQL template.
#                                Many queries repeat the same name twice because
#                                the SQL uses the parameter twice (e.g. once in a
#                                SELECT list alias and once in a WHERE clause).
#                                The consuming page zips this list with user-supplied
#                                values using Python's str.format() / positional %s.
#     "help":     str,         — one-line usage hint shown in the UI tooltip
# }
# ---------------------------------------------------------------------------
QUERY_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ============================
    # BAGGAGE
    # ============================
    "Baggage — Summary by PNR": {
        "category": "Baggage",
        "file": "baggage_summary_by_pnr.sql",
        "params": ["PNR_REF","PNR_REF",
                   "travlr_uci","travlr_uci",
                   "first_name","first_name",
                   "last_name","last_name",
                   "dep_port","dep_port",
                   "arr_port","arr_port"
                   ],
        "help": "Fast baggage details for a PNR (tag, weight, route, flight).",
    },
    "Baggage — By Name + Flight + Date Range": {
        "category": "Baggage",
        "file": "baggage_by_name_flight_daterange.sql",
        "params": [
            "last_name","last_name",
            "first_name","first_name",
            "airline","airline",
            "flight_no","flight_no",
            "dep_port","dep_port",
            "arr_port","arr_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Use when no PNR: customer provides Name + flight/route + date range.",
    },
    "Baggage — Bag Group + Pax Count (by Flight/Route)": {
        "category": "Baggage",
        "file": "baggage_group_pax_by_route.sql",
        "params": [
            "airline","airline",
            "flight_no","flight_no",
            "dep_port","dep_port",
            "arr_port","arr_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to",
            "cabin_code","cabin_code"
        ],
        "help": "Baggage details + pax count per booking for a route/flight/date (optional cabin code).",
    },

    # ============================
    # EXCESS
    # ============================
    "Excess/Prepaid — XBAG Items by PNR": {
        "category": "Excess",
        "file": "excess_xbag_by_pnr.sql",
        "params": ["PNR_REF"],
        "help": "Use when pax was charged/prepaid for excess baggage (amount/weight/pieces).",
    },
    "Excess — QHB0312 Excess Bag (by Traveller UCI / Date)": {
        "category": "Excess",
        "file": "excess_qhb0312_by_uci_date.sql",
        "params": [
            "travlr_uci","travlr_uci",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Direct lookup in QHB0312_EXCESS_BAG_NEW (best when XBAG doesn’t return records).",
    },

    # ============================
    # AUDIT
    # ============================
    "Audit — Bag Tag Print Location (TGNO)": {
        "category": "Audit",
        "file": "audit_bag_tag_print_tgno.sql",
        "params": [
            "PNR_REF","PNR_REF",
            "flight_no","flight_no",
            "dep_port","dep_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Bag tag print audit: tag number, workstation, user, timestamp (TGNO).",
    },
    "Bag — Check-in Location by BAG_UBI_ID / UCI": {
        "category": "Audit",
        "file": "bag_checkin_location_by_bag_ubi.sql",
        "params": ["bag_ubi_id", "travlr_uci", "travlr_uci"],
        "help": "Find check-in workstation/location for a bag (BAG_UBI_ID required; UCI optional).",
    },

    # ============================
    # CHECK-IN
    # ============================
    "Check-in — Channel/Workstation (QM10845)": {
        "category": "Check-in",
        "file": "checkin_channel_qm10845.sql",
        "params": [
            "PNR_REF","PNR_REF",
            "flight_no","flight_no",
            "dep_port","dep_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Check-in channel + workstation + user id (Channel/Location audit).",
    },

    # ============================
    # TICKET
    # ============================
    "PNR — Details by Ticket": {
        "category": "Ticket",
        "file": "pnr_by_ticket_number.sql",
        "params": ["PNR_REF","PNR_REF", "tkt_number", "tkt_number"],
        "help": "Ticket numbers & types for a PNR (optional filter by Ticket Number).",
    },

    # ============================
    # CM HISTORY
    # ============================
    "CM History — Lightweight (Keyword/Text/Time)": {
        "category": "CM History",
        "file": "cm_history_lightweight.sql",
        "params": [
            "PNR_REF","PNR_REF",
            "flight_no","flight_no",
            "dep_port","dep_port",
            "arr_port","arr_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to",
            "hist_keyword","hist_keyword"
        ],
        "help": "CM history without heavy decoding: keyword + text + user/office + timestamp. Optional filters supported.",
    },

    # ============================
    # OPS
    # ============================
    "Ops — Baggage Count by Injected Port": {
        "category": "Ops",
        "file": "ops_baggage_count_by_injected_port.sql",
        "params": [
            "airline",
            "flight_no","flight_no",
            "injected_port","injected_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Flight-wise baggage count injected at a specific journey start port (travelled only).",
    },
    "Ops — Booked vs Travelled Pax by Cabin": {
        "category": "Ops",
        "file": "ops_booked_vs_travelled_by_cabin.sql",
        "params": [
            "airline",
            "flight_no","flight_no",
            "dep_port","dep_port",
            "arr_port","arr_port",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Booked vs travelled pax counts split by cabin for airline/date range (optional flight/route).",
    },
    "Ops — Segment Count by Booking Ref & Traveller UCI": {
        "category": "Ops",
        "file": "ops_segment_count_by_booking_and_uci.sql",
        "params": [
            "booking_ref","booking_ref",
            "travlr_uci","travlr_uci",
            "dep_date_from","dep_date_from",
            "dep_date_to","dep_date_to"
        ],
        "help": "Counts segments for a traveller within a booking ref; labels DOM/INTL.",
    },
}