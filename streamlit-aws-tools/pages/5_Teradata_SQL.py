# pages/5_Teradata_SQL.py

import os
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
                st.image(str(lp), width='stretch')
    with c2:
        st.markdown("### Qantas — Teradata Workbench")
except Exception:
    pass

# -----------------------------
# JDBC options (from your prompt)
# -----------------------------
JDBC_OPTIONS = {
    "NonProd (AD_DTLK_INTEG)": "jdbc:teradata://teradata-nonprod.qcpaws.qantas.com.au/DATABASE=AD_DTLK_INTEG,LOGMECH=LDAP,DBS_PORT=1025",
    "Prod (P_DTLK_INTEG)": "jdbc:teradata://teradata-prod.qcpaws.qantas.com.au/DATABASE=P_DTLK_INTEG,LOGMECH=LDAP,DBS_PORT=1025",
}

def parse_teradata_jdbc(jdbc: str) -> Dict:
    """
    Parse JDBC style string:
      jdbc:teradata://<host>/<K=V, K=V,...>
    into dict: {host, database, logmech, dbs_port}
    """
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
        # Split by comma, each token K=V (ignore tokens w/o '=')
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

def connect_teradata(host: str, user: str, password: str,
                     database: Optional[str] = None, logmech: str = "LDAP",
                     dbs_port: int = 1025, encryptdata: Optional[bool] = None):
    """
    Establish a teradatasql connection. Returns a live connection or raises.
    """
    kwargs = {
        "host": host,
        "user": user,
        "password": password,
        "logmech": logmech,
        "dbs_port": dbs_port,
    }
    if database:
        kwargs["database"] = database
    # Optional TLS
    if encryptdata is not None:
        kwargs["encryptdata"] = encryptdata

    conn = teradatasql.connect(**kwargs)
    return conn

def run_query(conn, sql: str, params: Optional[tuple] = None, limit_rows: Optional[int] = None) -> pd.DataFrame:
    """
    Execute a SQL and return DataFrame. If limit_rows provided, trims after fetch.
    """
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
    """
    List tables/views in a database, filtered by name substring (case-insensitive).
    Uses DBC.TablesV and returns a DataFrame trimmed to `limit`.
    """
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
    """Set the default database for the session."""
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
    user = st.text_input("Username (LDAP)", value=st.session_state.get("td_user", ""), placeholder="e.g., ajay.kumar")
with c3:
    password = st.text_input("Password", value="", type="password", placeholder="Enter LDAP password")
with c4:
    encrypt_tls = st.checkbox("Encrypt (TLS)", value=True, help="Enable TLS encryption (encryptdata)")

# show parsed info
try:
    parsed = parse_teradata_jdbc(jdbc_selected)
    st.caption(f"Host: `{parsed['host']}` | Default DB: `{parsed.get('database') or '(none)'}'` | Logmech: `{parsed['logmech']}` | Port: `{parsed['dbs_port']}`")
except Exception as e:
    st.error(f"Failed to parse JDBC string: {e}")
    st.stop()

cc1, cc2, cc3 = st.columns([1, 1, 1])
with cc1:
    connect_btn = st.button("🔌 Connect", type="primary", width='stretch')
with cc2:
    reuse_btn = st.button("♻️ Reuse Existing (if still valid)", width='stretch')
with cc3:
    disconnect_btn = st.button("🔒 Disconnect", width='stretch')

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
    # Attempt to reuse by reconnecting with cached username (no password stored)
    # NOTE: we cannot reuse without password; so this button only validates an existing open connection.
    st.info("No active connection found to reuse. Please connect.")
    # If you have a corporate password vault, you could integrate reuse here.
    # For now, reuse only validates an already-open connection.

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
            # simple validation
            df_ts = run_query(conn, "SELECT CURRENT_TIMESTAMP AS NowTs")
            # set default database explicitly (in case needed)
            if parsed.get("database"):
                set_default_database(conn, parsed["database"])

            st.session_state["td_conn"] = conn
            st.session_state["td_ctx_db"] = parsed.get("database")
            st.session_state["td_user"] = user
            st.success(f"Connected to {parsed['host']} as {user}.")
            st.dataframe(df_ts, width='stretch')
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

    tabs = st.tabs(["📝 SQL Editor", "📚 Table Finder"])

    # -------------------------
    # SQL Editor Tab
    # -------------------------
    with tabs[0]:
        st.markdown("##### Run SQL")
        default_sql = "SELECT TOP 10 * FROM DBC.DatabasesV;"
        sql = st.text_area("SQL", value=st.session_state.get("td_last_sql", default_sql), height=200, placeholder="Write your SQL here...")
        limit_rows = st.number_input("Row cap (client-side)", min_value=1, max_value=100000, value=5000, step=100)
        c_run1, c_run2 = st.columns([1, 1])
        with c_run1:
            run_btn = st.button("▶️ Run", type="primary", width='stretch', key="run_sql_btn")
        with c_run2:
            clear_btn = st.button("🧹 Clear Output", width='stretch', key="clear_sql_btn")

        if clear_btn:
            st.session_state.pop("td_last_df", None)
            st.rerun()

        if run_btn:
            try:
                df = run_query(conn, sql, limit_rows=limit_rows)
                st.session_state["td_last_sql"] = sql
                st.session_state["td_last_df"] = df
                st.success(f"Returned {len(df)} rows (showing up to {limit_rows}).")
                if len(df) > 0:
                    st.dataframe(df, width='stretch')
                else:
                    st.info("Query returned no rows.")
            except Exception as e:
                st.error(f"Query failed: {e}")

        # Render last result if present
        if "td_last_df" in st.session_state and st.session_state["td_last_df"] is not None:
            with st.expander("Last result (persisted this session)", expanded=False):
                st.dataframe(st.session_state["td_last_df"], width='stretch')

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
            list_btn = st.button("🔎 List Tables", type="primary", width='stretch')
        with c_tf2:
            set_db_btn = st.button("📌 Set Default DB", width='stretch')

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
                        st.dataframe(df_tbls, width='stretch')
                except Exception as e:
                    st.error(f"Failed to list tables: {e}")

else:
    st.info("Not connected. Please provide credentials and click **Connect**.")