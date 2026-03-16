# pages/1_Teradata_Workbench.py
import io
import datetime as dt
from pathlib import Path
from typing import Optional, Tuple, Dict

import pandas as pd
import streamlit as st

# You need: pip install teradatasql
try:
    import teradatasql
except ImportError:
    st.error("Missing dependency: `teradatasql`. Install with: `pip install teradatasql`")
    st.stop()

# 🔽 NEW: import the query registry and loader
from sql.registry import QUERY_REGISTRY, load_sql

st.set_page_config(page_title="Teradata SQL", page_icon="🗄️", layout="wide")
st.title("🗄️ Teradata — SQL Workbench")

# -----------------------------
# Optional header with Qantas logo
# -----------------------------
try:
    c1, c2 = st.columns([0.12, 0.88])
    with c1:
        lp = Path("assets/qantas_logo.png")
        if lp.exists():
            try:
                st.logo(str(lp))   # Streamlit >= 1.31
            except Exception:
                st.image(str(lp))
    with c2:
        st.markdown("### Qantas — Teradata Workbench")
except Exception:
    pass

# -----------------------------
# JDBC options (from your prompt)
# -----------------------------
JDBC_OPTIONS = {
    "BIDSS (PQMF)": "jdbc:teradata://p.qantas.com.au/DATABASE=PQMF,LOGMECH=LDAP,DBS_PORT=1025"
}

def parse_teradata_jdbc(jdbc: str) -> Dict:
    jdbc = jdbc.strip()
    if not jdbc.lower().startswith("jdbc:teradata://"):
        raise ValueError("Invalid JDBC string. Must start with jdbc:teradata://")

    body = jdbc[len("jdbc:teradata://"):]
    if "/" not in body:
        host = body
        params_str = ""
    else:
        host, params_str = body.split("/", 1)

    params = {}
    if params_str:
        for token in params_str.split(","):
            token = token.strip()
            if "=" in token:
                k, v = token.split("=", 1)
                params[k.strip().upper()] = v.strip()

    return {
        "host": host,
        "database": params.get("DATABASE"),
        "logmech": params.get("LOGMECH", "LDAP"),
        "dbs_port": int(params.get("DBS_PORT", "1025")),
    }

def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return bio.getvalue()

def render_download_buttons(df: pd.DataFrame, base_name: str):
    if df is None or df.empty:
        return
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_base = "".join([c if c.isalnum() or c in ("-", "_") else "_" for c in base_name])[:60]
    filename_base = f"{safe_base}_{ts}"

    cdl1, cdl2 = st.columns(2)
    with cdl1:
        st.download_button(
            label="⬇️ Download CSV",
            data=_df_to_csv_bytes(df),
            file_name=f"{filename_base}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with cdl2:
        st.download_button(
            label="⬇️ Download Excel",
            data=_df_to_excel_bytes(df),
            file_name=f"{filename_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

def connect_teradata(host: str, user: str, password: str,
                     database: Optional[str] = None, logmech: str = "LDAP",
                     dbs_port: int = 1025, encryptdata: Optional[bool] = None):
    kwargs = {
        "host": host,
        "user": user,
        "password": password,
        "logmech": logmech,
        "dbs_port": dbs_port,
    }
    if database:
        kwargs["database"] = database
    if encryptdata is not None:
        kwargs["encryptdata"] = encryptdata

    conn = teradatasql.connect(**kwargs)
    return conn

def run_query(conn, sql: str, params: Optional[tuple] = None, limit_rows: Optional[int] = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description] if cur.description else []
    df = pd.DataFrame(rows, columns=cols)
    if limit_rows is not None and len(df) > limit_rows:
        df = df.head(limit_rows)
    return df

def list_tables(conn, database: str, name_filter: str = "", limit: int = 500) -> pd.DataFrame:
    sql = """
    SELECT
        DatabaseName,
        TableName,
        TableKind,
        CreateTimeStamp,
        LastAlterTimeStamp
    FROM DBC.TablesV
    WHERE DatabaseName = ?
      AND UPPER(TableName) LIKE UPPER(?)
    ORDER BY TableName
    """
    like = f"%{name_filter}%" if name_filter else "%"
    df = run_query(conn, sql, params=(database, like))
    if len(df) > limit:
        df = df.head(limit)
    return df

def set_default_database(conn, database: str):
    run_query(conn, f"DATABASE {database}")

# -----------------------------
# Connection Panel
# -----------------------------
st.markdown("#### Connection")

c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
with c1:
    env_label = st.selectbox("Environment / Host", options=list(JDBC_OPTIONS.keys()), index=0)
    jdbc_selected = JDBC_OPTIONS[env_label]
with c2:
    user = st.text_input("Username (LDAP)", key="td_user", placeholder="e.g., ajay.kumar")
with c3:
    password = st.text_input("Password", value="", type="password", placeholder="Enter LDAP password")
with c4:
    encrypt_tls = st.checkbox("Encrypt (TLS)", value=True, help="Enable TLS encryption (encryptdata)")

try:
    parsed = parse_teradata_jdbc(jdbc_selected)
    st.caption(f"Host: `{parsed['host']}` | Default DB: `{parsed.get('database') or '(none)'}'` | Logmech: `{parsed['logmech']}` | Port: `{parsed['dbs_port']}`")
except Exception as e:
    st.error(f"Failed to parse JDBC string: {e}")
    st.stop()

cc1, cc2, cc3 = st.columns([1, 1, 1])
with cc1:
    connect_btn = st.button("🔌 Connect", type="primary", use_container_width=True)
with cc2:
    reuse_btn = st.button("♻️ Reuse Existing (if still valid)", use_container_width=True)
with cc3:
    disconnect_btn = st.button("🔒 Disconnect", use_container_width=True)

# Maintain connection in session_state
if disconnect_btn and "td_conn" in st.session_state:
    try:
        st.session_state["td_conn"].close()
    except Exception:
        pass
    for k in ["td_conn", "td_ctx_db", "td_user"]:
        st.session_state.pop(k, None)
    st.success("Disconnected.")

if reuse_btn and "td_conn" not in st.session_state:
    st.info("No active connection found to reuse. Please connect.")

if connect_btn:
    if not user or not password:
        st.error("Please provide both username and password.")
    else:
        try:
            conn = connect_teradata(
                host=parsed["host"],
                user=user,
                password=password,
                database=parsed.get("database"),
                logmech=parsed.get("logmech", "LDAP"),
                dbs_port=parsed.get("dbs_port", 1025),
                encryptdata=True if encrypt_tls else None,
            )
            df_ts = run_query(conn, "SELECT CURRENT_TIMESTAMP AS NowTs")
            if parsed.get("database"):
                set_default_database(conn, parsed["database"])

            st.session_state["td_conn"] = conn
            st.session_state["td_ctx_db"] = parsed.get("database")
            st.success(f"Connected to {parsed['host']} as {user}.")
            st.dataframe(df_ts, use_container_width=True)
        except Exception as e:
            st.error(f"Connection failed: {e}")

# -----------------------------
# If connected, show tools
# -----------------------------
if "td_conn" in st.session_state:
    conn = st.session_state["td_conn"]
    current_db = st.session_state.get("td_ctx_db")

    st.markdown("---")
    st.markdown("#### Tools")

    tabs = st.tabs(["📝 SQL Editor", "📚 Table Finder", "Default Queries"])

    # -------------------------
    # SQL Editor Tab
    # -------------------------
    with tabs[0]:
        st.markdown("##### Run SQL")

        default_sql = "SELECT TOP 10 * FROM DBC.DatabasesV;"

        if "td_sql_editor" not in st.session_state:
            st.session_state["td_sql_editor"] = st.session_state.get("td_last_sql", default_sql)

        sql_text = st.text_area(
            "SQL",
            key="td_sql_editor",
            height=200,
            placeholder="Write your SQL here..."
        )

        limit_rows = st.number_input(
            "Row cap (client-side)",
            min_value=1,
            max_value=100000,
            value=5000,
            step=100,
            key="td_row_cap"
        )

        c_run1, c_run2 = st.columns([1, 1])
        with c_run1:
            run_btn = st.button("▶️ Run", type="primary", use_container_width=True, key="run_sql_btn")
        with c_run2:
            clear_btn = st.button("🧹 Clear Output", use_container_width=True, key="clear_sql_btn")

        if clear_btn:
            st.session_state.pop("td_last_df", None)
            st.session_state.pop("td_last_sql", None)
            st.session_state["td_sql_editor"] = default_sql
            st.rerun()

        if run_btn:
            try:
                df = run_query(conn, sql_text, limit_rows=limit_rows)
                st.session_state["td_last_sql"] = sql_text
                st.session_state["td_last_df"] = df

                st.success(f"Returned {len(df)} rows (showing up to {limit_rows}).")
                if not df.empty:
                    st.dataframe(df, use_container_width=True)
                    render_download_buttons(df, base_name="sql_output")
                else:
                    st.info("Query returned no rows.")
            except Exception as e:
                st.error(f"Query failed: {e}")

        if st.session_state.get("td_last_df") is not None:
            with st.expander("Last query & result", expanded=False):
                if st.session_state.get("td_last_sql"):
                    st.code(st.session_state["td_last_sql"], language="sql")

                df_last = st.session_state.get("td_last_df")
                if df_last is not None:
                    st.dataframe(df_last, use_container_width=True)
                    render_download_buttons(df_last, base_name="sql_last_result")

    # -------------------------
    # Table Finder Tab
    # -------------------------
    with tabs[1]:
        st.markdown("##### Search Tables")
        col_tf1, col_tf2, col_tf3 = st.columns([1, 1, 1])
        with col_tf1:
            db_for_search = st.text_input("Database", value=current_db or "", placeholder="e.g., AD_DTLK_INTEG")
        with col_tf2:
            name_filter = st.text_input("Table name contains", value="", placeholder="e.g., customer")
        with col_tf3:
            tf_limit = st.number_input("Max rows", min_value=1, max_value=20000, value=500, step=100)

        c_tf1, c_tf2 = st.columns([1, 1])
        with c_tf1:
            list_btn = st.button("🔎 List Tables", type="primary", use_container_width=True)
        with c_tf2:
            set_db_btn = st.button("📌 Set Default DB", use_container_width=True)

        if set_db_btn:
            if not db_for_search.strip():
                st.error("Provide a database name to set as default.")
            else:
                try:
                    set_default_database(conn, db_for_search.strip())
                    st.session_state["td_ctx_db"] = db_for_search.strip()
                    st.success(f"Default database set to {db_for_search.strip()}.")
                except Exception as e:
                    st.error(f"Failed to set default database: {e}")

        if list_btn:
            if not db_for_search.strip():
                st.error("Please provide a Database name to search.")
            else:
                try:
                    df_tbls = list_tables(conn, db_for_search.strip(), name_filter=name_filter.strip(), limit=tf_limit)
                    if len(df_tbls) == 0:
                        st.info("No tables found matching the criteria.")
                    else:
                        st.success(f"Found {len(df_tbls)} object(s).")
                        st.dataframe(df_tbls, use_container_width=True)
                except Exception as e:
                    st.error(f"Failed to list tables: {e}")

    # -------------------------
    # Default Queries Tab (refactored)
    # -------------------------
    with tabs[2]:
        st.markdown("##### Frequently used Queries (Baggage / Check-in / Excess / Booking / CM / Ticket / Ops)")
        st.caption("Empty inputs are treated as NULL and filters are skipped. All widgets use unique keys.")

        KEY_PREFIX = "tdq_"

        # -------------- UI INPUTS --------------
        st.markdown("#### 🔎 Filters (used by quick queries)")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            PNR_REF = st.text_input("PNR Reference", value="", placeholder="e.g. 6UO3NT", key=f"{KEY_PREFIX}pnr")
        with c2:
            last_name = st.text_input("Traveller Last Name", value="", placeholder="Optional", key=f"{KEY_PREFIX}last")
        with c3:
            first_name = st.text_input("Traveller First Name", value="", placeholder="Optional", key=f"{KEY_PREFIX}first")
        with c4:
            airline = st.text_input("Airline", value="QF", placeholder="e.g. QF", key=f"{KEY_PREFIX}airline")

        c5, c6, c7, c8 = st.columns(4)
        with c5:
            flight_no = st.text_input("Flight Number", value="", placeholder="e.g. 0361", key=f"{KEY_PREFIX}flt")
        with c6:
            dep_port = st.text_input("Departure Port", value="", placeholder="e.g. SYD", key=f"{KEY_PREFIX}dep")
        with c7:
            arr_port = st.text_input("Arrival Port", value="", placeholder="e.g. LAX", key=f"{KEY_PREFIX}arr")
        with c8:
            injected_port = st.text_input("Baggage Journey Start Port", value="", placeholder="e.g. LST (optional)", key=f"{KEY_PREFIX}inj")

        c9, c10, c11 = st.columns(3)
        with c9:
            dep_date_from = st.date_input("Departure Date From", value=None, key=f"{KEY_PREFIX}dt_from")
        with c10:
            dep_date_to = st.date_input("Departure Date To", value=None, key=f"{KEY_PREFIX}dt_to")
        with c11:
            quick_cap = st.number_input("Row cap", min_value=10, max_value=200000, value=5000, step=100, key=f"{KEY_PREFIX}cap")

        with st.expander("Advanced Inputs (only needed for some queries)", expanded=False):
            a1, a2, a3, a4 = st.columns(4)
            with a1:
                travlr_uci = st.text_input("Traveller UCI", value="", placeholder="e.g. 20062FC80000C56D", key=f"{KEY_PREFIX}uci")
            with a2:
                bag_ubi_id = st.text_input("Bag UBI ID", value="", placeholder="e.g. 1234567890", key=f"{KEY_PREFIX}bag_ubi")
            with a3:
                hist_keyword = st.text_input("CM Hist Keyword (optional)", value="", placeholder="e.g. TGNO / ACCH / BRDS", key=f"{KEY_PREFIX}hist_kw")
            with a4:
                tkt_number = st.text_input("Ticket Number (optional)", value="", placeholder="e.g. 0812345678901", key=f"{KEY_PREFIX}tkt")

            booking_ref = st.text_input(
                "Booking Ref (BKG_REF_NO / PNR) for QMU queries",
                value=(PNR_REF or ""),
                placeholder="e.g. 3CUEVE",
                key=f"{KEY_PREFIX}bkgref"
            )

            cabin_code = st.text_input(
                "Cabin Code (optional)",
                value="",
                placeholder="e.g. Y/J/W/P",
                key=f"{KEY_PREFIX}cabin"
            )

        # Helpers
        def _val(x: str):
            x = (x or "").strip()
            return x if x else None

        def _date(d):
            return d if d else None

        UI = dict(
            PNR_REF=_val(PNR_REF),
            last_name=_val(last_name),
            first_name=_val(first_name),
            airline=_val(airline),
            flight_no=_val(flight_no),
            dep_port=_val(dep_port),
            arr_port=_val(arr_port),
            injected_port=_val(injected_port),
            dep_date_from=_date(dep_date_from),
            dep_date_to=_date(dep_date_to),
            travlr_uci=_val(travlr_uci),
            bag_ubi_id=_val(bag_ubi_id),
            hist_keyword=_val(hist_keyword),
            tkt_number=_val(tkt_number),
            booking_ref=_val(booking_ref),
            cabin_code=_val(cabin_code),
        )

        # ------------------------ CATEGORY FILTER ------------------------
        st.markdown("---")
        categories = sorted(set(v["category"] for v in QUERY_REGISTRY.values()))
        cat = st.selectbox("Category", options=["All"] + categories, index=0, key=f"{KEY_PREFIX}cat")

        filtered_names = [k for k, v in QUERY_REGISTRY.items() if (cat == "All" or v["category"] == cat)]

        st.session_state.setdefault(f"{KEY_PREFIX}last_sql", "")
        st.session_state.setdefault(f"{KEY_PREFIX}last_df", None)
        st.session_state.setdefault(f"{KEY_PREFIX}last_name", "")

        def run_named_query(name: str):
            meta = QUERY_REGISTRY[name]
            sql_text = load_sql(meta["file"])
            params = [UI[p] for p in meta["params"]]
            df = run_query(conn, sql_text, params=tuple(params), limit_rows=quick_cap)

            st.session_state[f"{KEY_PREFIX}last_sql"] = sql_text
            st.session_state[f"{KEY_PREFIX}last_df"] = df
            st.session_state[f"{KEY_PREFIX}last_name"] = name

            st.success(f"✅ Ran: {name}")
            st.caption(meta.get("help", ""))

            if df.empty:
                st.info("No rows returned.")
            else:
                st.dataframe(df, use_container_width=True)
                render_download_buttons(df, base_name=name.replace(" ", "_"))

        # --------------------------- BUTTON GRID ---------------------------
        st.markdown("#### ▶️ Quick Run")
        cols_per_row = 3
        for i in range(0, len(filtered_names), cols_per_row):
            row = st.columns(cols_per_row)
            for j in range(cols_per_row):
                idx = i + j
                if idx >= len(filtered_names):
                    continue
                name = filtered_names[idx]
                btn_key = f"{KEY_PREFIX}btn_{idx}_{name}"
                if row[j].button(f"▶️ {name}", use_container_width=True, key=btn_key):
                    run_named_query(name)

        # --------------------------- LAST RESULT ---------------------------
        with st.expander("Last quick query & result", expanded=False):
            last_name_run = st.session_state.get(f"{KEY_PREFIX}last_name", "")
            if last_name_run:
                st.markdown(f"**Last query:** {last_name_run}")

            if st.session_state.get(f"{KEY_PREFIX}last_sql"):
                st.code(st.session_state[f"{KEY_PREFIX}last_sql"], language="sql")

            df_last = st.session_state.get(f"{KEY_PREFIX}last_df")
            if df_last is not None:
                st.dataframe(df_last, use_container_width=True)
                render_download_buttons(df_last, base_name="quick_last_result")

else:
    st.info("Not connected. Please provide credentials and click **Connect**.")