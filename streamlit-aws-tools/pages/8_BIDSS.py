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
    "BIDSS (PQMF)": "jdbc:teradata://p.qantas.com.au/DATABASE=PQMF,LOGMECH=LDAP,DBS_PORT=1025"}

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

    tabs = st.tabs(["📝 SQL Editor", "📚 Table Finder", "Default Queries"])

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


    with tabs[2]:
        st.markdown("##### Frequently used Queries")

        # ===============================
        # 🔖 Frequently Used Queries — 10 buttons
        # ===============================
        st.subheader("✨ Quick Run: Frequently used queries")

        # You can later replace SQL strings (and labels) below with your real ones.
        # {key: (label, sql)}
        FREQUENT_QUERIES: dict[str, tuple[str, str]] = {
            "q1":  ("Top 10 Databases",           "SELECT TOP 10 DatabaseName, OwnerName FROM DBC.DatabasesV ORDER BY DatabaseName;"),
            "q2":  ("Booking Data with PNR", """ SELECT 
                                                    DISTINCT qt.PNR_REF -- PNR REF
                                                    , qt.TRAVLR_LAST_NAME -- Last Name of Passenger
                                                    , qt.TRAVLR_FIRST_NAME -- First Name of Passenger
                                                    , qb.BAG_UBI_ID AS BAG_SYSTEM_ID -- Baggage UBI ID
                                                    , RIGHT('0000' || TRIM (BAG_TAG_CARRIER_NUMBER), 4) || '-' || TRIM(BAG_TAG_NUMBER) AS TAG_NUMBER -- BAGTAG
                                                    , BAG_DEST_PORT_CODE AS DEST -- Final Destination of Baggage
                                                    , qb.BAG_WGT AS WEIGHT_KG -- Weight in KG
                                                    FROM PQMF.QHBS300_BAG_GROUP qbg
                                                    INNER JOIN PQMF.QHB0303_BAG_GROUP_MEMBER qbgm
                                                    ON qbg.BAG_GROUP_BR_ID = qbgm.BAG_GROUP_BR_ID
                                                    INNER JOIN PQMF.QHBS304_BAG qb
                                                    ON qbgm.BAG_UBI_ID = qb.BAG_UBI_ID
                                                    LEFT JOIN PQMF.QHBS200_TRAVELLER qt
                                                    ON qbg.TRAVLR_UCI = qt.TRAVLR_UCI
                                                    WHERE qt.PNR_REF  = COALESCE(?, qt.PNR_REF) -- Update PNR to extract baggage data"""),
            "q3":  ("CM History of Passengers",    """
                                                    SEL distinct
                                                    t200.pnr_ref
                                                    ,t200.TRAVLR_last_NAME
                                                    ,t200.TRAVLR_FIRST_NAME
                                                --     ,t200.TRAVLR_DATE_OF_BIRTH          
                                                    ,t240.AAIRALPC  
                                                    ,t240.PRMEFLTN
                                                    -- ,T241.Accept_Channel_Code
                                                    ,T405.Alloc_Seat_Ref_Text
                                                    ,T221.BKD_CABIN_CODE
                                                    --   ,T221.BKD_CLASS_CODE

                                                -- ,X.FMFL_ID
                                                ,t240.DEPUPOR
                                                ,t240.ARVLPOR   
                                                ,t240.LOCLDEPD
                                                --,X.CABIN
                                                --,X.BKD_CLASS_CODE
                                                --,X.TIER_LEVEL_CODE
                                                --,X.SEAT_PREFERENCE
                                                --,X.ALLOC_SEAT_REF_TEXT
                                                --,X.PNR_REF
                                                --,X.TRAVLR_UPI
                                                ,T261.HIST_KEYWRD_CODE
                                                ,
                                                COALESCE(
                                                CASE
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACAI' THEN 'ADVANCED ACCEPTANCE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACFR' THEN 'FREEZE ACCEPTANCE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACFO' THEN 'FORCE ACCEPTANCE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACCC' THEN 'ACCEPTANCE CABIN'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACCH' THEN 'ACCEPTANCE CHANNEL'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACRC' THEN 'ACCEPTANCE CANCELLATION REASON'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACRT' THEN 'ACCEPTANCE FREE TEXT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACSC' THEN 'ACCEPTANCE SECURITY NUMBER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ACST' THEN 'ACCEPTANCE STATUS'
                                                WHEN  T261.HIST_KEYWRD_CODE  ='APDC' THEN 'ADVANCED PAX. DIRECTIVE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APII' THEN 'APIS DATA'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APIS' THEN 'AQQ STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APSE' THEN 'ESTA RESULT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APSI' THEN 'MANUAL UPDATE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APSV' THEN 'VALID DOCUMENT FOR TRAVEL'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APPC' THEN 'PARTICIPATING COUNTRY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APCR' THEN 'COUNTRY OF RESIDENCE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='APSC' THEN 'APPLICATION STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='BGCL' THEN 'BAGGAGE CLASS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='BRDC' THEN 'BOARDING CHANNEL'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='BRDP' THEN 'BRDP'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='BRDM' THEN 'MASS BOARDED'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='BRDS' THEN 'BOARDING STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CAGE' THEN 'CUSTOMER DETAILS-AGE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CAGU' THEN 'CUSTOMER DETAILS-AGE UNIT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CCCO' THEN 'CREDIT CARD-COMPANY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CCED' THEN 'CREDIT CARD-EXPIRY DATE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CCNM' THEN 'CREDIT CARD-NUMBER'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CCRF' THEN 'CREATED FORM'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CTLT' THEN 'PRODUCT BAGGAGE-TITLE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CDOB' THEN 'CUSTOMER DETAILS-DATE OF BIRTH'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CAGU' THEN 'CUSTOMER DETAILS-AGE UNIT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CGEN' THEN 'CUSTOMER DETAILS-GENDER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CBLI' THEN 'CUSTOMER FRAUD-BLACKLISTED'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CODS' THEN 'DELIVERY STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='COPR' THEN 'COMMENT PRIORITY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='COTP' THEN 'COMMENT TYPE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='COTX' THEN 'COMMENT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CGVN' THEN 'CUSTOMER FIRST NAME'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CMFN' THEN 'MASTER FIRST NAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CMLN' THEN 'LAST NAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CMRL' THEN 'MASTER RECORD LOCATOR'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CMTL' THEN 'MASTER TITLE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CSRN' THEN 'CUSTOMER SURNAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CTYP' THEN 'CUSTOMER TYPE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CTFL' THEN 'CUSTOMER TRACKING DETAILS-LOCATION'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CTRD' THEN 'CUSTOMER TRACKING DETAILS-DEVICE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CTRC' THEN 'CUSTOMER TRACKING DETAILS-CHANNEL'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CMGT' THEN 'MERGE TYPE'    
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='CMFN' THEN 'MASTER FIRST NAME'    
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ETDR' THEN 'E-TICKET DISASSOCIATION REASON'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ETCS' THEN 'E-TICKET STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ETCN' THEN 'E-TICKET COUPON'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ETAT' THEN 'ASSOCIATION REASON'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ETNM' THEN 'TICKET NUMBER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBBA' THEN 'EXCESS BAGGAGE INTINERARY- BAGGAG ALLOWANCE'  
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBAC' THEN 'EXCESS BAGGAGE- ACCEPTED AMOUNT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBCH' THEN 'EXCESS BAGGAGE- EXCESS CHARGED'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBEC' THEN 'EXCESS BAGGAGE- EXCESS CHARGE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBEX' THEN 'EXCESS BAGGAGE- EXCESS AMOUNT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBIT' THEN 'EXCESS BAGGAGE- EXCESS ITINERARY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBRA' THEN 'EXCESS BAGGAGE- RATE PER UNIT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBAU' THEN 'EXCESS BAGGAGE- AUTHORIZED BY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBFW' THEN 'EXCESS BAGGAGE ITINERARY- FULL WAIVE AMOUNT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBRS' THEN 'EXCESS BAGGAGE - REASON'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBWE' THEN 'EXCESS BAGGAGE - WAIVED EXCESS CHARGE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBWI' THEN 'EXCESS BAGGAGE ITINERARY - WAIVED EXCESS CHARGE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBST' THEN 'EXCESS BAGGAGE- STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBDN' THEN 'EXCESS BAGGAGE- DOCUMENT NUMBER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBPE' THEN 'EXCESS BAGGAGE- PAID EXCESS CHARGE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBPI' THEN 'EXCESS BAGGAGE ITINERARY- PAID EXCESS CHARGE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='EBFW' THEN 'EXCESS BAGGAGE ITINERARY-FULL WAIVE AMOUNT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='ECDD' THEN 'EMERGENCY CONTACT DETAILS DECLINED'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='FQTC' THEN 'FQTV-AIRLINE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='FQTN' THEN 'FQTV-FQTV NUMBER'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='FQTT' THEN 'FQTV-TIER'      
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='FQTA' THEN 'FQTV-AIRLINES TIER'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='FQTU' THEN 'FQTV-USAGE'    
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='HOBW' THEN 'HOLD BAGGAGE WEIGHT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='HOBN' THEN 'HOLD BAGGAGE PIECES'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='HABN' THEN 'CABIN BAGGAGE PIECES'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='HABW' THEN 'CABIN BAGGAGE WEIGHT'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='HOPN' THEN 'BAGGAGE POOL'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='IATE' THEN 'IATCI ERROR'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='LKEM' THEN 'LINK ERROR'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='LNCD' THEN 'LINKED CUSTOMERS'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='LNND' THEN 'CUSTOMERS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='UNIT' THEN 'BAG UNITS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='OFFP' THEN 'OFFP'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='OCNM' THEN 'OWNING CUSTOMER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='PRCA' THEN 'PRODUCT DETAILS-CABIN'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='PRBC' THEN 'PRODUCT DETAILS-CLASS'  
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='PRBS' THEN 'PRODUCT DETAILS-STATUS'      
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='PRTS' THEN 'BOARDING PASS PRINT STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='PRTR' THEN 'IGNORE TRAFFIC RESTRICTIONS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='PRNA' THEN 'NATIONALITY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RAUP' THEN 'AUTHORISED BY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RCSF' THEN 'CUSTOMER FIRST NAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RCSS' THEN 'CUSTOMER SURNAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RCST' THEN 'CUSTOMER TITLE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDCO' THEN 'COUNTRY(DOCUMENTS)'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDDS' THEN 'SOURCE OF DATA'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RCAD' THEN 'DELIVERED CABIN'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RLOC' THEN 'RECORD LOCATOR'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDCI' THEN 'COUNTRY OF ISSUE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDED' THEN 'EXPIRY DATE(DOCUMENT)'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDID' THEN 'REGULATORY DOCUMENT-DATE OF ISSUE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGAC' THEN 'REGULATORY ADDRESS-CITY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGAL' THEN 'REGULATORY ADDRESS-COUNTRY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGAS' THEN 'REGULATORY ADDRESS-ADDRESS_STREET NAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGAT' THEN 'REGULATORY ADDRESS-TYPE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGAP' THEN 'REGULATORY ADDRESS-PROVINCE/COUNTY/STATE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGAZ' THEN 'REGULATORY ADDRESS-POST/ZIP CODE'  
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDFN' THEN 'REGULATORY DOCUMENT-FIRST NAME'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDMN' THEN 'REGULATORY DOCUMENT-MIDDLENAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDNO' THEN 'DOCUMENT NUMBER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDSN' THEN 'SURNAME'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RDTP' THEN 'DOCUMENT TYPE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGCA' THEN 'PROPOSED CABIN'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGCA' THEN 'PROPOSED CABIN'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGCA' THEN 'REGRADE PROPOSED CABIN'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGCP' THEN 'REGRADE REASON'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGOS' THEN 'REGRADE ONLOAD STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGOP' THEN 'ONLOAD PRIORITY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGPP' THEN 'REGRADE PRIORITY'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGST' THEN 'REGRADE STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGTP' THEN 'REGRADE EXTRA INFORMATION'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='RGTY' THEN 'REGRADE TYPE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='STGN' THEN 'SEAT NUMBER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='STOS' THEN 'OVERRIDE SUITABILITY REASON'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='STGS' THEN 'SEATING STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='STGS' THEN 'SEATING STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='STPF' THEN 'SEATING PREFERENCE'   
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='STPU' THEN 'SEAT PREFERENCE UPDATE REASON'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='SVCC' THEN 'SERVICE CODE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='SVCD' THEN 'SERVICE EXTRA INFORMATION'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='SVCE' THEN 'DECODE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='SVCN' THEN 'NUMBER REQUIRED'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='SVCS' THEN 'SERVICE STATUS'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='SVCT' THEN 'SVCT'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='TGIC' THEN 'BAGGAGE ISSUING CARRIER'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='TGNO' THEN 'BAG TAG'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='TGPC' THEN 'BAG DESTINATION'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='TGST' THEN 'BAG TAG SOURCE'
                                                    WHEN  T261.HIST_KEYWRD_CODE  ='UEEX' THEN 'EMERGENCY EXIT'
                                                    ELSE
                                                T261.HIST_KEYWRD_CODE
                                                END,'') AS HIST_KEYWORD_CODE_
                                                ,COALESCE(T261.HIST_IMAGE_TYPE_CODE,'') AS HIST_IMAGE_TYPE_CODE          
                                                ,COALESCE(CASE
                                                                    WHEN T261.HIST_TEXT='CR' AND  T261.HIST_KEYWRD_CODE  ='ACRC' THEN 'CUSTOMER REQUEST'/* THIS IS TO BE  EDITED*/
                                                                    WHEN T261.HIST_TEXT='J' AND  T261.HIST_KEYWRD_CODE  ='BRDC'THEN 'JFE'
                                                                    WHEN T261.HIST_TEXT='K' AND  T261.HIST_KEYWRD_CODE  ='ACCH' THEN 'KIOSK'
                                                                    WHEN T261.HIST_TEXT='J' AND  T261.HIST_KEYWRD_CODE  ='ACCH'  THEN 'JFE'
                                                                    WHEN T261.HIST_TEXT='A' AND  T261.HIST_KEYWRD_CODE  ='ACST' THEN 'ACCEPTED'
                                                                    WHEN T261.HIST_TEXT='N' AND  T261.HIST_KEYWRD_CODE  ='ACST' THEN 'NOT ACCEPTED'
                                                                    WHEN T261.HIST_TEXT='N' AND  T261.HIST_KEYWRD_CODE  ='ACAI' THEN 'NO'
                                                                    WHEN T261.HIST_TEXT='A' AND  T261.HIST_KEYWRD_CODE  ='ETAT'  THEN 'ASSOCIATED'
                                                                    WHEN T261.HIST_TEXT='D' AND  T261.HIST_KEYWRD_CODE  ='ETAT' THEN 'DISASSOCIATED'
                                                                    WHEN T261.HIST_TEXT='NP' AND T261.HIST_KEYWRD_CODE  ='HOPN'THEN 'NOT POOLED'
                                                                    WHEN T261.HIST_TEXT='MOP' AND T261.HIST_KEYWRD_CODE  ='HOPN'THEN 'MEMBER OF POOL'
                                                                    WHEN T261.HIST_TEXT='HOP' AND T261.HIST_KEYWRD_CODE  ='HOPN'THEN 'HEAD OF POOL'
                                                                    WHEN T261.HIST_TEXT='P' AND  T261.HIST_KEYWRD_CODE  ='PRTS' THEN 'PRINTED'
                                                                    WHEN T261.HIST_TEXT='N' AND  T261.HIST_KEYWRD_CODE  ='PRTS' THEN 'NEEDS PRINTING'
                                                                    WHEN T261.HIST_TEXT='B' AND  T261.HIST_KEYWRD_CODE  ='BRDS' THEN 'BOARDED'
                                                                    WHEN T261.HIST_TEXT='N' AND  T261.HIST_KEYWRD_CODE  ='BRDS' THEN 'NOT BOARDED'
                                                                    WHEN T261.HIST_TEXT='H' AND T261.HIST_KEYWRD_CODE  ='COPR' THEN 'HIGH'
                                                                    WHEN T261.HIST_TEXT='C' AND T261.HIST_KEYWRD_CODE  ='CTYP' THEN 'CHILD'
                                                                    WHEN T261.HIST_TEXT='A' AND T261.HIST_KEYWRD_CODE  ='CTYP' THEN 'ADULT'
                                                                    WHEN T261.HIST_TEXT='N' AND T261.HIST_KEYWRD_CODE  ='CODS' THEN 'NOT DELIVERED'
                                                                    WHEN T261.HIST_TEXT='D' AND T261.HIST_KEYWRD_CODE  ='CODS' THEN 'DELIVERED'
                                                                    WHEN T261.HIST_TEXT='U' AND  T261.HIST_KEYWRD_CODE  ='EBST' THEN 'UNPAID'
                                                                    WHEN T261.HIST_TEXT='P' AND  T261.HIST_KEYWRD_CODE  ='EBST' THEN 'PAID'
                                                                    WHEN T261.HIST_TEXT='XF' AND T261.HIST_KEYWRD_CODE  ='ETDR' THEN 'CANCELLED'
                                                                    WHEN T261.HIST_TEXT='C' AND T261.HIST_KEYWRD_CODE  ='ETCS'THEN 'CHECKED IN'
                                                                    WHEN T261.HIST_TEXT='O' AND T261.HIST_KEYWRD_CODE  ='ETCS'THEN 'OPEN'
                                                                    WHEN T261.HIST_TEXT='A' AND T261.HIST_KEYWRD_CODE  ='FQTU'THEN 'ACCRUAL'
                                                                    WHEN T261.HIST_TEXT='Y' AND T261.HIST_KEYWRD_CODE  ='BRDM'THEN 'YES'
                                                                    WHEN T261.HIST_TEXT='N' AND T261.HIST_KEYWRD_CODE  ='STGS' THEN 'NOT GUARANTEED'
                                                                    WHEN T261.HIST_TEXT='G' AND T261.HIST_KEYWRD_CODE  ='STGS' THEN 'GUARANTEED'
                                                                    ELSE    T261.HIST_TEXT  END,'') AS HIST_TEXT_
                                                ,X.USER_SONIC_ID
                                                ,X.OFFICE_ID
                                                ,COALESCE((T263.AIRPORT_CODE || ' '|| T263.TERMINAL_CODE|| ' ' || T263.LOCATION_CATG_CODE || ' '  ||
                                                T263.AIRPORT_OR_CITY_IND || ' ' ||T263.WORKSTATION_LOCATION_INDEX ),
                                                (T313.AIRPORT_CODE || ' '|| T313.TERMINAL_CODE|| ' ' || T313.LOCATION_CATG_CODE || ' '  ||
                                                T313.AIRPORT_OR_CITY_IND || ' ' ||T313.WORKSTATION_LOCATION_INDEX ),'')   AS WORKSTATION_ID
                                                ,X.ACTIVITY_TSMP
                                                ,T261.HIST_TRANS_ID
                                                ,t261.travlr_upi
                                                -- ,COALESCE(T261.TRANS_SEQ_NO,'')       AS TRANS_SEQ_NUM
                                                --,t263.*
                                                ,x.travlr_uci
                                                -- ,T221.TRAVLR_UCI
                                                FROM        PQMF.QHB0240_FLIGHT_LEG_DEPART T240
                                                INNER JOIN PQMF.QHB0241_TRAVLR_FLIGHT_LEG T241
                                                ON   T240.FMFL_ID                =       T241.FMFL_ID
                                                AND  T240.FL_VERSION_NO          =       T241.FL_VERSION_NO
                                                INNER JOIN PQMF.QHB0221_FLIGHT_ITINERARY  T221
                                                ON   T241.TRAVLR_UPI=T221.TRAVLR_UPI
                                                AND  T241.TRAVLR_UCI=T221.TRAVLR_UCI
                                                INNER JOIN PQMF.QHBS200_TRAVELLER T200
                                                ON   T200.PNR_REF =T221.PNR_REF
                                                AND        T200.TRAVLR_UCI=T221.TRAVLR_UCI
                                                LEFT JOIN PQMF.QHB0405_TRAVLR_SEAT_ALLOC T405
                                                ON  T221.TRAVLR_UCI  = T405.TRAVLR_UCI   
                                                AND T221.TRAVLR_UPI =  T405.TRAVLR_UPI  
                                                AND T241.FMFL_ID=T405.FMFL_ID
                                                INNER  JOIN PQMF.QHB0260_CM_HISTORY_TRAN       x
                                                ON x.TRAVLR_UCI=T241.TRAVLR_UCI         
                                                AND  x.FMFL_ID=T240.FMFL_ID  
                                                AND x.FL_VERSION_NO =T240.FL_VERSION_NO     
                                                --FROM    PQMF.QHB0260_CM_HISTORY_TRAN        X
                                                inner JOIN     PQMF.QHB0261_CM_HISTORY T261
                                                ON     X.HIST_TRANS_ID  =    T261.HIST_TRANS_ID
                                                AND      X.FMFL_ID       =    T261.FMFL_ID
                                                AND x.FL_VERSION_NO      =    T261.FL_VERSION_NO
                                                AND X.TRAVLR_UCI=T261.TRAVLR_UCI
                                                and x.TRANS_SEQ_NO = t261.TRANS_SEQ_NO
                                                AND t241.TRAVLR_UPI=T261.TRAVLR_UPI
                                                AND t240.DEPUPOR=T261.DEPUPOR
                                                LEFT JOIN        PQMF.QHB0263_CHECKIN_LOCATION   T263
                                                ON  t261.TRAVLR_UCI         =  T263.TRAVLR_UCI
                                                AND t261.TRAVLR_UPI=T263.TRAVLR_UPI
                                                AND t261.FL_VERSION_NO      =    T263.FL_VERSION_NO
                                                AND t261.FMFL_ID       =    T263.FMFL_ID
                                                AND t261.HIST_TRANS_ID      =T263.HIST_TRANS_ID
                                                and t261.TRANS_SEQ_NO = t263.TRANS_SEQ_NO
                                                AND t240.DEPUPOR=T263.AIRPORT_CODE
                                                left join PQMF.QHB0313_BAG_CHECKIN_LOCATION t313
                                                ON  --t261.TRAVLR_UCI         =  T313.TRAVLR_UCI
                                                --AND t261.TRAVLR_UPI=T313.TRAVLR_UPI
                                                --AND
                                                t261.FL_VERSION_NO      =    T313.FL_VERSION_NO
                                                AND t261.FMFL_ID       =    T313.FMFL_ID
                                                AND t261.HIST_TRANS_ID      =T313.HIST_TRANS_ID
                                                and t261.TRANS_SEQ_NO = t313.TRANS_SEQ_NO
                                                AND t240.DEPUPOR=T313.AIRPORT_CODE
                                                --LEFT JOIN PQMF.QHB0262_TRAVLR_ACC_HIST       T262
                                                -- ON X.HIST_TRANS_ID  =    T262.HIST_TRANS_ID
                                                -- AND X.TRAVLR_UCI = T262.TRAVLR_UCI
                                                -- AND t241.TRAVLR_UPI= T262.TRAVLR_UPI
                                                -- AND x.FMFL_ID = T262.FMFL_ID
                                                --  AND X.ARVLPOR=T262.ARVLPOR
                                                -- AND X.DEPUPOR=T262.DEPUPOR
                                                WHERE

                                                --------------Do Not Touch ----------------------
                                                T261.HIST_IMAGE_TYPE_CODE='NEW'  -- Captures latest transactions only      
                                                AND HIST_KEYWORD_CODE_ NOT LIKE '%Document%' -- Filter Passpost data
                                                AND HIST_KEYWRD_CODE NOT IN ('RDDS','RDSN','RDCI','RDCA','RDCI') -- Filtered Regulatory Data
                                                --------------Do Not Touch ------------------------


                                                    -- Optional filters (pass NULL to skip each one)
                                                    AND T200.PNR_REF      = COALESCE(?, T200.PNR_REF)       -- pnr_ref
                                                    AND T240.PRMEFLTN     = COALESCE(?, T240.PRMEFLTN)      -- flight_no
                                                    AND T240.DEPUPOR      = COALESCE(?, T240.DEPUPOR)       -- dep_port
                                                    AND T240.ARVLPOR      = COALESCE(?, T240.ARVLPOR)       -- arr_port
                                                    AND T240.LOCLDEPD     >= COALESCE(?, T240.LOCLDEPD)     -- dep_date_from
                                                    AND T240.LOCLDEPD     <= COALESCE(?, T240.LOCLDEPD)     -- dep_date_to
                                                    AND T200.TRAVLR_LAST_NAME  = COALESCE(?, T200.TRAVLR_LAST_NAME)   -- last_name
                                                    AND T200.TRAVLR_FIRST_NAME = COALESCE(?, T200.TRAVLR_FIRST_NAME)  -- first_name


                                                qualify dense_RANK() OVER (partition by T261.HIST_TRANS_ID,t261.travlr_upi order by case when t261.travlr_upi = t241.travlr_upi then 1 else 2 end asc, t261.fmfl_id asc) = 1
                                                order by x.activity_tsmp asc, T261.HIST_TRANS_ID asc,t263.HIST_SUB_CAT_CODE asc, t240.LOCLDEPD                """),
            "q4":  ("Large Tables (Top 50)",      "SELECT TOP 50 DatabaseName, TableName, CurrentPerm AS Bytes FROM DBC.TablesizeV ORDER BY CurrentPerm DESC;"),
            "q5":  ("User Sessions (sample)",     "SEL TOP 50 * FROM DBC.SessionInfoV ORDER BY CollectTimeStamp DESC;"),
            "q6":  ("Locking (sample)",           "SEL TOP 50 * FROM DBC.LockInfoV ORDER BY TimeAcquired DESC;"),
            "q7":  ("Tables by Pattern",          "SELECT TOP 200 DatabaseName, TableName FROM DBC.TablesV WHERE UPPER(TableName) LIKE UPPER('%{pattern}%') ORDER BY 1,2;"),
            "q8":  ("Columns by Pattern",         "SELECT TOP 200 DatabaseName, TableName, ColumnName FROM DBC.ColumnsV WHERE UPPER(ColumnName) LIKE UPPER('%{pattern}%') ORDER BY 1,2,3;"),
            "q9":  ("Stats Summary (sample)",     "SEL TOP 100 DatabaseName, TableName, StatsId, LastCollectTimeStamp FROM DBC.StatsV ORDER BY LastCollectTimeStamp DESC;"),
            "q10": ("DBC Version Info",           "SEL * FROM DBC.DBCInfoV;"),
        }

# ----------------- OPTIONAL FILTER INPUTS (Approach 1 COALESCE) -----------------

        st.markdown("#### ✈️ BIDSS Optional Filters")

        colp1, colp2, colp3, colp4 = st.columns(4)
        colp5, colp6, colp7, colp8 = st.columns(4)

        with colp1:
            PNR_REF = st.text_input("PNR Reference", value="", placeholder="PNR_REF")

        with colp2:
            flight_no = st.text_input("Flight Number", value="", placeholder="e.g. 0528")

        with colp3:
            dep_port = st.text_input("Departure Port", value="", placeholder="e.g. SYD")

        with colp4:
            arr_port = st.text_input("Arrival Port", value="", placeholder="e.g. AKL")

        with colp5:
            dep_date_from = st.date_input("Departure Date From", value=None)

        with colp6:
            dep_date_to = st.date_input("Departure Date To", value=None)

        with colp7:
            last_name = st.text_input("Traveller Last Name", value="", placeholder="Optional")

        with colp8:
            first_name = st.text_input("Traveller First Name", value="", placeholder="Optional")

        quick_cap = st.number_input(
            "Row cap for quick queries",
            min_value=10, max_value=200000,
            value=int(tf_limit),
            step=100
        )

        # ------------------ FREQUENT QUERY BUTTONS ------------------

        rows = [
            ["q1", "q2", "q3", "q4", "q5"],
            ["q6", "q7", "q8", "q9", "q10"],
        ]

        st.session_state.setdefault("bidss_last_sql", "")
        st.session_state.setdefault("bidss_last_df", None)

        # 1) Map each quick query key to the EXACT parameter names it expects, IN ORDER of ? placeholders
        #    For example, q2 only has ONE placeholder for PNR (qt.PNR_REF = COALESCE(?, qt.PNR_REF))
        PARAM_SCHEMA: dict[str, list[str]] = {
            # examples — adjust to match your SQL templates:
            "q1":  [],
            "q2":  ["PNR_REF"],  # <-- your "Booking Data with PNR" query expects only PNR
            "q3":  ["PNR_REF", "flight_no", "dep_port", "arr_port", "dep_date_from", "dep_date_to", "last_name", "first_name"],  # sample
            "q4":  ["dep_port", "arr_port", "dep_date_from", "dep_date_to"],  # sample
            "q5":  ["PNR_REF", "flight_no"],  # sample
            "q6":  ["PNR_REF", "last_name", "first_name"],  # sample
            "q7":  ["dep_date_from", "dep_date_to"],  # sample
            "q8":  ["dep_port"],  # sample
            "q9":  ["arr_port"],  # sample
            "q10": ["PNR_REF"],  # sample
        }

        # 2) Helper to build a param list according to the schema
        def _build_params(schema: list[str]) -> list:
            # Map UI field names to current values (convert dates to ISO if you prefer; teradatasql will accept date/datetime objects too)
            value_map = {
                "PNR_REF": PNR_REF or None,
                "flight_no": flight_no or None,
                "dep_port": dep_port or None,
                "arr_port": arr_port or None,
                "dep_date_from": (dep_date_from if dep_date_from else None),  # can also use dep_date_from.isoformat()
                "dep_date_to": (dep_date_to if dep_date_to else None),
                "last_name": last_name or None,
                "first_name": first_name or None,
            }
            return [value_map[name] for name in schema]

        # 3) Run button handler with the correct parameter list for each qkey
        def _run_quick_query(qkey: str):
            label, sql_template = FREQUENT_QUERIES[qkey]

            # Pick the schema for this query; if missing, default to zero params
            schema = PARAM_SCHEMA.get(qkey, [])
            params = _build_params(schema)

            try:
                # IMPORTANT: we pass 'params=params' so teradatasql binds the ? placeholders.
                # The length of 'params' must match the number of ? in sql_template.
                df = run_query(conn, sql_template, params=params, limit_rows=quick_cap)

                st.session_state["bidss_last_sql"] = sql_template
                st.session_state["bidss_last_df"] = df

                st.success(f"✅ Ran: {label}")
                if len(df) == 0:
                    st.info("No rows returned.")
                else:
                    st.dataframe(df, width='stretch')
            except Exception as e:
                st.error(f"Query failed: {e}")

        # 4) Render buttons (unchanged)
        for r in rows:
            cols = st.columns(len(r))
            for i, qkey in enumerate(r):
                label = FREQUENT_QUERIES[qkey][0]
                if cols[i].button(f"▶️ {label}", width='stretch', key=f"btn_{qkey}"):
                    _run_quick_query(qkey)

        # 5) Show last result (unchanged)
        with st.expander("Last quick query & result", expanded=False):
            if st.session_state["bidss_last_sql"]:
                st.code(st.session_state["bidss_last_sql"], language="sql")
            df_last = st.session_state.get("bidss_last_df")
            if df_last is not None:
                st.dataframe(df_last, width='stretch')
            else:
                st.caption("No quick query run yet.")
else:
    st.info("Not connected. Please provide credentials and click **Connect**.")