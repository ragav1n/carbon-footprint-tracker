import os, io, re, json, base64, hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import pymssql
import sqlalchemy as sa
from sqlalchemy.engine import URL
from dateutil import parser as dtparser

import pdfplumber
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

# =========================
# App & environment config
# =========================
st.set_page_config(page_title="Azure Carbon Footprint Tracker", layout="wide")

SQL_SERVER = os.getenv("SQL_SERVER", "carbon-sql-server.database.windows.net")
SQL_DB     = os.getenv("SQL_DB", "carbon_tracker_db")
SQL_USER   = os.getenv("SQL_USER", "sqladminuser")
SQL_PASS   = os.getenv("SQL_PASS", "Ragava@2005")
TDS_VER    = os.getenv("TDS_VERSION", "7.4")

# Storage for PDFs
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
UPLOADS_CONTAINER = os.getenv("UPLOADS_CONTAINER", "uploads")

# Build SQLAlchemy engine (bulk inserts) using pymssql/FreeTDS
ENGINE_URL = URL.create(
    "mssql+pymssql",
    username=SQL_USER,
    password=SQL_PASS,
    host=SQL_SERVER,
    port=1433,
    database=SQL_DB,
    query={"tds_version": TDS_VER, "charset": "UTF-8"},
)
engine = sa.create_engine(ENGINE_URL, pool_pre_ping=True)

# Blob client (optional for local if not set)
blob_service = None
if AZURE_STORAGE_CONNECTION_STRING:
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

# =========================
# DB helpers
# =========================
def _conn():
    return pymssql.connect(
        server=SQL_SERVER, user=SQL_USER, password=SQL_PASS,
        database=SQL_DB, port=1433, tds_version=TDS_VER, charset="UTF-8"
    )

@st.cache_data(ttl=300)
def load_footprint():
    with _conn() as c:
        df = pd.read_sql("SELECT * FROM dbo.vw_carbon_footprint", c)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def insert_activity(row: dict):
    q = """
    INSERT INTO dbo.activity_logs
      (user_id, [date], category, activity, unit, quantity, source_system, ts_ingested, source_doc_id)
    VALUES
      (%(user_id)s, %(date)s, %(category)s, %(activity)s, %(unit)s, %(quantity)s, %(source_system)s, %(ts_ingested)s, %(source_doc_id)s)
    """
    with _conn() as c:
        with c.cursor(as_dict=True) as cur:
            cur.execute(q, row)
        c.commit()

def bulk_insert_activities(df: pd.DataFrame):
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "ts_ingested" in df.columns:
        df["ts_ingested"] = pd.to_datetime(df["ts_ingested"])
    # ensure source_doc_id column exists (nullable)
    if "source_doc_id" not in df.columns:
        df["source_doc_id"] = None
    with engine.begin() as con:
        df.to_sql("activity_logs", con, schema="dbo", if_exists="append", index=False, method="multi", chunksize=1000)

def ensure_doc_tables():
    ddl = """
    IF OBJECT_ID('dbo.source_documents','U') IS NULL
    CREATE TABLE dbo.source_documents (
      doc_id           UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
      user_id          NVARCHAR(64)     NOT NULL,
      doc_type         NVARCHAR(32)     NOT NULL,
      period_start     DATE             NULL,
      period_end       DATE             NULL,
      storage_url      NVARCHAR(1024)   NULL,
      storage_path     NVARCHAR(512)    NULL,
      sha256           CHAR(64)         NULL,
      parsed_json      NVARCHAR(MAX)    NULL,
      status           NVARCHAR(32)     NOT NULL DEFAULT 'parsed',
      created_utc      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
    );

    IF COL_LENGTH('dbo.activity_logs','source_doc_id') IS NULL
      ALTER TABLE dbo.activity_logs ADD source_doc_id UNIQUEIDENTIFIER NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_docs_user_created')
      CREATE INDEX IX_docs_user_created ON dbo.source_documents(user_id, created_utc DESC);
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(ddl)
        c.commit()

def insert_source_document(user_id, doc_type, period_start, period_end, storage_url, storage_path, sha256, parsed_json, status='parsed'):
    q = """
      INSERT INTO dbo.source_documents
        (user_id, doc_type, period_start, period_end, storage_url, storage_path, sha256, parsed_json, status)
      OUTPUT INSERTED.doc_id
      VALUES
        (%(user_id)s,%(doc_type)s,%(period_start)s,%(period_end)s,%(storage_url)s,%(storage_path)s,%(sha256)s,%(parsed_json)s,%(status)s)
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(q, {
                "user_id": user_id,
                "doc_type": doc_type,
                "period_start": period_start,
                "period_end": period_end,
                "storage_url": storage_url,
                "storage_path": storage_path,
                "sha256": sha256,
                "parsed_json": json.dumps(parsed_json) if isinstance(parsed_json, (dict, list)) else parsed_json,
                "status": status
            })
            row = cur.fetchone()
        c.commit()
    return row[0]

# =========================
# Storage helpers (PDFs)
# =========================
def upload_pdf_and_get_sas(user_id: str, filename: str, content: bytes) -> tuple[str, str]:
    if not blob_service:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not configured.")
    today = pd.Timestamp.utcnow()
    blob_path = f"uploads/{user_id}/{today.year}/{today.month:02d}/{filename}"
    blob_client = blob_service.get_blob_client(container=UPLOADS_CONTAINER, blob=blob_path)
    blob_client.upload_blob(content, overwrite=True)

    # account key available when using connection string auth
    account_name = blob_service.account_name
    account_key = blob_service.credential.account_key
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=UPLOADS_CONTAINER,
        blob_name=blob_path,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=today + timedelta(days=1)
    )
    sas_url = f"https://{account_name}.blob.core.windows.net/{UPLOADS_CONTAINER}/{blob_path}?{sas}"
    return blob_path, sas_url

# =========================
# PDF parsing heuristics
# =========================
DATE_PAT = r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})'
KWH_PAT  = r'(\d+(?:\.\d+)?)\s*kwh'

def parse_electricity_bill(pdf_bytes: bytes) -> dict:
    text_all = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            text_all.append(t)
    txt = "\n".join(text_all).lower()

    kwh = None
    for m in re.finditer(KWH_PAT, txt, re.IGNORECASE):
        try:
            val = float(m.group(1))
            kwh = max(kwh or 0.0, val)  # pick largest as "total"
        except:
            pass

    dates = []
    for m in re.finditer(DATE_PAT, txt):
        dates.append(m.group(1))
    norm_dates = []
    for d in dates:
        try:
            norm_dates.append(pd.to_datetime(d, dayfirst=True).date())
        except:
            pass
    period_start, period_end = (None, None)
    if len(norm_dates) >= 2:
        period_start = min(norm_dates[0], norm_dates[1])
        period_end   = max(norm_dates[0], norm_dates[1])

    excerpt = "\n".join((text_all[0][:500] if text_all else ""))
    return {"kwh": kwh, "period_start": period_start, "period_end": period_end, "text_excerpt": excerpt}

# =========================
# UI: load data + sidebar
# =========================
st.title("🌍 Azure-Powered Carbon Footprint Tracker")

with st.sidebar:
    st.header("Filters")
    ensure_doc_tables()
    df_all = load_footprint()
    if df_all.empty:
        st.info("No data yet. Add activities, upload CSV, or upload a bill PDF.")
        min_d = date.today()
        max_d = date.today()
        cats = []
        users = []
    else:
        min_d = min(df_all["date"])
        max_d = max(df_all["date"])
        cats  = sorted(df_all["category"].dropna().unique().tolist())
        users = sorted(df_all["user_id"].dropna().unique().tolist())

    date_range = st.date_input("Date range", (min_d, max_d), min_value=min_d, max_value=max_d)
    sel_cats = st.multiselect("Category", options=cats, default=cats)
    sel_users = st.multiselect("User", options=users, default=users)

    st.markdown("---")
    st.caption("Database")
    st.text(f"Server: {SQL_SERVER}")
    st.text(f"DB:     {SQL_DB}")

# filter data for dashboard views
if not df_all.empty:
    start_d, end_d = (date_range if isinstance(date_range, tuple) else (min_d, max_d))
    mask = (
        (df_all["date"] >= start_d)
        & (df_all["date"] <= end_d)
        & (df_all["category"].isin(sel_cats) if sel_cats else True)
        & (df_all["user_id"].isin(sel_users) if sel_users else True)
    )
    df = df_all.loc[mask].copy()
else:
    df = df_all.copy()

# =========================
# Tabs
# =========================
tab_dash, tab_add, tab_upload, tab_reco, tab_pdf, tab_docs = st.tabs(
    ["📊 Dashboard", "➕ Add Activity", "📤 Upload CSV", "💡 Recommendations", "📑 Upload PDF (Bills)", "📚 Documents"]
)

# ---- Dashboard ----
with tab_dash:
    if df.empty:
        st.warning("No rows match the current filters.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        total = df["kg_co2e"].sum()
        per_act = df["kg_co2e"].mean()
        daily = df.groupby("date")["kg_co2e"].sum()
        last7 = daily.tail(7).sum() if len(daily) else 0
        c1.metric("Total CO₂e (kg)", f"{total:,.2f}")
        c2.metric("Avg per activity (kg)", f"{per_act:.2f}")
        c3.metric("Last 7 days (kg)", f"{last7:,.2f}")
        c4.metric("Activities", f"{len(df):,}")

        st.subheader("By Category")
        st.bar_chart(df.groupby("category")["kg_co2e"].sum())

        st.subheader("Daily Trend")
        ddf = daily.reset_index().sort_values("date")
        st.line_chart(ddf.set_index("date")["kg_co2e"])

        st.subheader("Monthly Rollup")
        tmp = df.copy()
        tmp["date"] = pd.to_datetime(tmp["date"])
        tmp["month"] = tmp["date"].dt.to_period("M").dt.to_timestamp()
        monthly = tmp.groupby("month")["kg_co2e"].sum().reset_index()
        st.bar_chart(monthly.set_index("month")["kg_co2e"])

        st.subheader("Rows")
        st.dataframe(df.sort_values("date", ascending=False), use_container_width=True, height=420)
        st.download_button("⬇️ Download filtered CSV", data=df.to_csv(index=False).encode("utf-8"),
                           file_name="carbon_filtered.csv", mime="text/csv")

# ---- Add Activity ----
with tab_add:
    st.markdown("Add a single activity row to the database.")
    with st.form("add_activity"):
        col1, col2, col3 = st.columns(3)
        user_id  = col1.text_input("User ID", value=(sel_users[0] if sel_users else "u_001"))
        date_in  = col2.date_input("Date", value=date.today())
        category = col3.selectbox("Category", options=["transport", "electricity", "waste", "procurement", "other"], index=1)

        col4, col5, col6 = st.columns(3)
        activity = col4.text_input("Activity", value="grid_kwh" if category=="electricity" else "")
        unit     = col5.text_input("Unit", value="kwh" if category=="electricity" else "")
        quantity = col6.number_input("Quantity", min_value=0.0, step=0.1)

        source_system = st.text_input("Source system", value="manual")
        ts_ingested   = datetime.utcnow().isoformat(timespec="seconds")

        submitted = st.form_submit_button("Add row")
        if submitted:
            if not user_id or not activity or not unit:
                st.error("Please fill User ID, Activity, and Unit.")
            else:
                try:
                    row = {
                        "user_id": user_id, "date": date_in, "category": category,
                        "activity": activity, "unit": unit, "quantity": float(quantity),
                        "source_system": source_system, "ts_ingested": ts_ingested,
                        "source_doc_id": None
                    }
                    insert_activity(row)
                    st.success("Row inserted ✅")
                    load_footprint.clear()
                except Exception as e:
                    st.error(f"Insert failed: {e}")

# ---- Upload CSV ----
with tab_upload:
    st.markdown("Upload a CSV with **activity_logs** schema:")
    st.code("user_id,date,category,activity,unit,quantity,source_system,ts_ingested", language="text")

    up = st.file_uploader("Choose CSV", type=["csv"])
    if up:
        try:
            df_up = pd.read_csv(up)
            required = {"user_id","date","category","activity","unit","quantity"}
            missing = required - set(df_up.columns)
            if missing:
                st.error(f"Missing required columns: {', '.join(sorted(missing))}")
            else:
                st.dataframe(df_up.head(20), use_container_width=True)
                if st.button("Append to database"):
                    bulk_insert_activities(df_up)
                    st.success(f"Inserted {len(df_up):,} rows ✅")
                    load_footprint.clear()
        except Exception as e:
            st.error(f"Upload failed: {e}")

# ---- Recommendations ----
with tab_reco:
    st.markdown("Simple rule-based suggestions (upgradeable to Azure ML / Azure OpenAI).")
    if df.empty:
        st.info("No data to analyze.")
    else:
        tmp = pd.DataFrame(df)
        tmp["date"] = pd.to_datetime(tmp["date"])
        last30_cut = tmp["date"].max() - pd.Timedelta(days=30)
        last30 = tmp[tmp["date"] >= last30_cut]

        cat_tot = last30.groupby("category")["kg_co2e"].sum().sort_values(ascending=False)
        st.write("**Last 30 days CO₂e by category (kg):**")
        st.dataframe(cat_tot.reset_index().rename(columns={"kg_co2e":"kg_last_30"}), use_container_width=True)

        sug = []
        if "electricity" in cat_tot.index and cat_tot["electricity"] > 5:
            sug.append("Switch to LED lighting, star-rated appliances; consider rooftop solar where feasible.")
        if "transport" in cat_tot.index and cat_tot["transport"] > 5:
            sug.append("Prefer public transit / carpool / rail; consolidate trips; consider EV/hybrid.")
        if "waste" in cat_tot.index and cat_tot["waste"] > 2:
            sug.append("Increase recycling and composting; audit high-waste items.")
        if "procurement" in cat_tot.index and cat_tot["procurement"] > 2:
            sug.append("Choose local suppliers, low-packaging SKUs, and recycled materials.")
        if not sug:
            sug = ["Keep current habits — marginal improvements possible (optimize standby loads, efficient routing)."]

        st.markdown("### Suggested Actions")
        for i, s in enumerate(sug, 1):
            st.write(f"{i}. {s}")
        st.caption("Note: Impact estimates are illustrative; replace with Azure ML / curated factors for accuracy.")

# ---- Upload PDF (Bills) ----
with tab_pdf:
    st.markdown("### Upload Utility Bill (PDF) → auto-parse kWh → store PDF & insert activity")
    user_default = (sel_users[0] if sel_users else "u_001")
    user_id_for_pdf = st.text_input("User ID for this bill", value=user_default)

    up_pdf = st.file_uploader("Choose a PDF bill", type=["pdf"])
    if up_pdf is not None:
        pdf_bytes = up_pdf.read()
        sha = hashlib.sha256(pdf_bytes).hexdigest()

        with st.expander("Preview first-page text (debug)"):
            try:
                parsed = parse_electricity_bill(pdf_bytes)
                st.code(parsed["text_excerpt"] or "(no text found)")
            except Exception as e:
                parsed = {"kwh": None, "period_start": None, "period_end": None, "text_excerpt": ""}
                st.warning(f"Parse attempt failed: {e}")

        st.write("**Parsed fields (you can edit):**")
        kwh = st.number_input("Total kWh", value=float(parsed["kwh"] or 0.0), min_value=0.0, step=0.1)
        pstart = st.date_input("Period start", value=parsed["period_start"] or pd.Timestamp.utcnow().date())
        pend   = st.date_input("Period end",   value=parsed["period_end"]   or pd.Timestamp.utcnow().date())

        colA, colB = st.columns(2)
        with colA:
            if st.button("Store PDF in Blob & Insert Activity"):
                try:
                    if not blob_service:
                        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not configured on server.")
                    # 1) upload & SAS
                    blob_path, sas_url = upload_pdf_and_get_sas(user_id_for_pdf, up_pdf.name, pdf_bytes)
                    # 2) doc record
                    doc_id = insert_source_document(
                        user_id=user_id_for_pdf,
                        doc_type="electricity_bill",
                        period_start=pstart,
                        period_end=pend,
                        storage_url=sas_url,
                        storage_path=blob_path,
                        sha256=sha,
                        parsed_json={"kwh": kwh, "period_start": str(pstart), "period_end": str(pend)}
                    )
                    # 3) activity linked to doc
                    row = {
                        "user_id": user_id_for_pdf,
                        "date": pend,
                        "category": "electricity",
                        "activity": "grid_kwh",
                        "unit": "kwh",
                        "quantity": float(kwh),
                        "source_system": "pdf_bill",
                        "ts_ingested": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
                        "source_doc_id": str(doc_id)
                    }
                    insert_activity(row)
                    st.success("PDF stored and activity inserted ✅")
                    load_footprint.clear()
                except Exception as e:
                    st.error(f"Ingestion failed: {e}")

        with colB:
            # inline PDF preview (small docs). Large files: rely on SAS link in Documents tab.
            b64 = base64.b64encode(pdf_bytes).decode()
            st.markdown(
                f'<iframe src="data:application/pdf;base64,{b64}" width="100%" height="420"></iframe>',
                unsafe_allow_html=True
            )

# ---- Documents tab ----
with tab_docs:
    st.markdown("### Recent uploaded documents")
    with _conn() as c:
        docs = pd.read_sql("""
            SELECT TOP 100 doc_id, user_id, doc_type, period_start, period_end, storage_url, storage_path, status, created_utc
            FROM dbo.source_documents
            ORDER BY created_utc DESC
        """, c)
    st.dataframe(docs, use_container_width=True, height=420)
    if not docs.empty:
        st.caption("Open a document via the SAS URL (valid ~1 day).")

