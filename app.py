from dotenv import load_dotenv
load_dotenv()

import os, io, re, json, base64, hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import pymssql
import sqlalchemy as sa
from sqlalchemy.engine import URL
from dateutil import parser as dtparser

import pdfplumber
import requests
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

# Ollama config (local AI recommendations)
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "llama3.2")
OLLAMA_ENABLED  = os.getenv("OLLAMA_ENABLED", "true").lower() == "true"

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

# Blob client
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

    IF OBJECT_ID('dbo.ai_recommendations','U') IS NULL
    CREATE TABLE dbo.ai_recommendations (
      reco_id              INT IDENTITY(1,1) PRIMARY KEY,
      user_filter          NVARCHAR(256) NULL,
      period_start         DATE NOT NULL,
      period_end           DATE NOT NULL,
      model                NVARCHAR(64) NOT NULL,
      summary_text         NVARCHAR(MAX) NULL,
      recommendation_text  NVARCHAR(MAX) NOT NULL,
      created_utc          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    IF OBJECT_ID('dbo.reports','U') IS NULL
    CREATE TABLE dbo.reports (
      report_id       INT IDENTITY(1,1) PRIMARY KEY,
      user_filter     NVARCHAR(256) NULL,
      period_start    DATE NOT NULL,
      period_end      DATE NOT NULL,
      title           NVARCHAR(256) NOT NULL,
      report_markdown NVARCHAR(MAX) NOT NULL,
      created_utc     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
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

LITRE_PAT = r'(\d+(?:\.\d+)?)\s*(l|litre|liter|liters|litres)\b'
KG_PAT    = r'(\d+(?:\.\d+)?)\s*(kg|kilogram|kilograms)\b'

def parse_fuel_receipt(pdf_bytes: bytes) -> dict:
    text_all = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            text_all.append(t)
    txt = "\n".join(text_all).lower()

    fuel_type = None
    if "diesel" in txt:
        fuel_type = "diesel"
    elif "petrol" in txt or "gasoline" in txt:
        fuel_type = "petrol"

    litres = None
    for m in re.finditer(LITRE_PAT, txt):
        try:
            val = float(m.group(1))
            litres = max(litres or 0.0, val)
        except:
            pass

    dates = []
    for m in re.finditer(DATE_PAT, txt):
        try:
            dates.append(pd.to_datetime(m.group(1), dayfirst=True).date())
        except:
            pass
    doc_date = dates[0] if dates else None

    excerpt = text_all[0][:500] if text_all else ""
    return {
        "litres": litres,
        "fuel_type": fuel_type,
        "date": doc_date,
        "text_excerpt": excerpt
    }

def parse_waste_invoice(pdf_bytes: bytes) -> dict:
    text_all = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            text_all.append(t)
    txt = "\n".join(text_all).lower()

    waste_type = "mixed"
    if "organic" in txt:
        waste_type = "organic"
    if "plastic" in txt:
        waste_type = "plastic"

    kg = None
    for m in re.finditer(KG_PAT, txt):
        try:
            val = float(m.group(1))
            kg = max(kg or 0.0, val)
        except:
            pass

    dates = []
    for m in re.finditer(DATE_PAT, txt):
        try:
            dates.append(pd.to_datetime(m.group(1), dayfirst=True).date())
        except:
            pass
    doc_date = dates[0] if dates else None

    excerpt = text_all[0][:500] if text_all else ""
    return {
        "kg": kg,
        "waste_type": waste_type,
        "date": doc_date,
        "text_excerpt": excerpt
    }

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
            kwh = max(kwh or 0.0, val)
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


def insert_ai_recommendation(user_filter, period_start, period_end, model, summary_text, recommendation_text):
    q = """
      INSERT INTO dbo.ai_recommendations
        (user_filter, period_start, period_end, model, summary_text, recommendation_text)
      OUTPUT INSERTED.reco_id
      VALUES
        (%(user_filter)s, %(period_start)s, %(period_end)s, %(model)s, %(summary_text)s, %(recommendation_text)s)
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(q, {
                "user_filter": user_filter,
                "period_start": period_start,
                "period_end": period_end,
                "model": model,
                "summary_text": summary_text,
                "recommendation_text": recommendation_text,
            })
            row = cur.fetchone()
        c.commit()
    return row[0]


def insert_report(user_filter, period_start, period_end, title, report_markdown):
    q = """
      INSERT INTO dbo.reports
        (user_filter, period_start, period_end, title, report_markdown)
      OUTPUT INSERTED.report_id
      VALUES
        (%(user_filter)s, %(period_start)s, %(period_end)s, %(title)s, %(report_markdown)s)
    """
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(q, {
                "user_filter": user_filter,
                "period_start": period_start,
                "period_end": period_end,
                "title": title,
                "report_markdown": report_markdown,
            })
            row = cur.fetchone()
        c.commit()
    return row[0]


def get_latest_ai_reco_for_filter(user_filter: str):
    q = """
      SELECT TOP 1 reco_id, period_start, period_end, model, summary_text, recommendation_text, created_utc
      FROM dbo.ai_recommendations
      WHERE user_filter = %(user_filter)s
      ORDER BY created_utc DESC
    """
    with _conn() as c:
        df = pd.read_sql(q, c, params={"user_filter": user_filter})
    if df.empty:
        return None
    return df.iloc[0]


# =========================
# Ollama helpers (AI recos)
# =========================
def call_ollama_chat(system_prompt: str, user_content: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "stream": False,
    }
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/chat"
    resp = requests.post(url, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    msg = (data.get("message") or {}).get("content", "").strip()
    if not msg:
        raise RuntimeError("Empty response from Ollama.")
    return msg

def generate_ai_recommendations_ollama(df_window: pd.DataFrame):
    """
    Returns (ai_text, summary_text, period_start, period_end)
    """
    if df_window.empty:
        return (
            "There is no emissions data in this period. Ask the user to add activities or upload bills/receipts first.",
            "",
            None,
            None,
        )

    df_local = df_window.copy()
    df_local["date"] = pd.to_datetime(df_local["date"])

    total = df_local["kg_co2e"].sum()
    by_cat = df_local.groupby("category")["kg_co2e"].sum().sort_values(ascending=False)
    by_act = (
        df_local
        .groupby(["category", "activity"])["kg_co2e"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    start = df_local["date"].min().date()
    end   = df_local["date"].max().date()

    summary_lines = [
        f"Time window: {start} to {end}",
        f"Total emissions (kg CO2e): {total:.2f}",
        "",
        "Emissions by category (kg CO2e):",
    ]
    for cat, val in by_cat.items():
        summary_lines.append(f"- {cat}: {val:.2f}")

    summary_lines.append("")
    summary_lines.append("Top activities by emissions:")
    for (cat, act), val in by_act.items():
        summary_lines.append(f"- {cat} / {act}: {val:.2f} kg CO2e")

    summary = "\n".join(summary_lines)

    system_prompt = (
        "You are a sustainability and carbon-footprint coach for individuals and small businesses in India. "
        "Given recent emissions data, explain in clear, friendly language what the user's footprint looks like, "
        "where the main hotspots are, and provide 5–8 concrete, practical recommendations to reduce emissions. "
        "Be specific but realistic (e.g., public transport, LED lighting, avoiding unnecessary trips, efficient appliances). "
        "Do not mention that you are an AI or that you received a summary text. Just speak directly to the user."
    )

    user_msg = (
        "Here is a summary of my recent carbon emissions:\n\n"
        f"{summary}\n\n"
        "Based on this, please analyze my situation and give me personalized recommendations to reduce my emissions. "
        "Group them under short headings (like 'Electricity', 'Transport', 'Waste') and keep the answer under 400 words."
    )

    ai_text = call_ollama_chat(system_prompt, user_msg)
    return ai_text, summary, start, end

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
tab_dash, tab_add, tab_upload, tab_reco, tab_pdf, tab_docs, tab_reports = st.tabs(
    ["📊 Dashboard", "➕ Add Activity", "📤 Upload CSV", "💡 Recommendations", "📑 Upload PDF", "📚 Documents", "📄 Reports"]
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
                        "user_id": user_id,
                        "date": date_in,
                        "category": category,
                        "activity": activity,
                        "unit": unit,
                        "quantity": float(quantity),
                        "source_system": source_system,
                        "ts_ingested": ts_ingested,
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
    st.markdown("Simple rule-based suggestions and AI-powered recommendations.")

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

        # Rule-based quick suggestions
        sug = []
        if "electricity" in cat_tot.index and cat_tot["electricity"] > 5:
            sug.append("Switch to LED lighting, star-rated appliances; consider rooftop solar where feasible.")
        if "transport" in cat_tot.index and cat_tot["transport"] > 5:
            sug.append("Prefer public transit / carpool / rail; consolidate trips; consider EV/hybrid where possible.")
        if "waste" in cat_tot.index and cat_tot["waste"] > 2:
            sug.append("Increase recycling and composting; audit high-waste items and packaging.")
        if "procurement" in cat_tot.index and cat_tot["procurement"] > 2:
            sug.append("Choose local suppliers, low-packaging SKUs, and recycled materials.")
        if not sug:
            sug = ["Keep current habits — focus on small optimizations (phantom loads, efficient routing, avoiding idle equipment)."]

        st.markdown("### Rule-based Suggested Actions")
        for i, s in enumerate(sug, 1):
            st.write(f"{i}. {s}")
        st.caption("These are simple heuristics. For smarter, personalized advice, use the AI-powered recommendations below.")

        st.markdown("---")
        st.markdown("### ✨ AI-Powered Recommendations (Ollama)")
        if not OLLAMA_ENABLED:
            st.info("AI recommendations are available only in the local version of this app.")
        else:
            st.caption(f"Using local model: `{OLLAMA_MODEL}` at `{OLLAMA_BASE_URL}`")

        # Encode current user filter as label
        if sel_users:
            user_filter_label = ",".join(sel_users)
        else:
            user_filter_label = "ALL"

        ai_placeholder = st.empty()

        if st.button("Generate AI recommendations"):
            with st.spinner("Talking to your local AI coach..."):
                try:
                    ai_text, summary_text, period_start, period_end = generate_ai_recommendations_ollama(last30)
                    ai_placeholder.markdown(ai_text)

                    if period_start and period_end:
                        reco_id = insert_ai_recommendation(
                            user_filter=user_filter_label,
                            period_start=period_start,
                            period_end=period_end,
                            model=OLLAMA_MODEL,
                            summary_text=summary_text,
                            recommendation_text=ai_text,
                        )
                        st.success(f"AI recommendations saved (ID: {reco_id}) ✅")
                except Exception as e:
                    ai_placeholder.empty()
                    st.error(f"AI recommendation failed: {e}")
                    st.caption(
                        "Check that Ollama is running (`ollama serve`) and the model is pulled "
                        "(for example, `ollama pull llama3.2`)."
                    )

        st.markdown("---")
        st.markdown("### 📄 Generate & Save Report for this period")

        if st.button("📄 Generate & Save Report"):
            if last30.empty:
                st.error("No data in the last 30 days for the current filters.")
            else:
                with st.spinner("Generating summary report..."):
                    try:
                        # Try to reuse latest AI reco for this filter
                        latest = get_latest_ai_reco_for_filter(user_filter_label)
                        if latest is None:
                            # If none exists, generate now
                            ai_text, summary_text, period_start, period_end = generate_ai_recommendations_ollama(last30)
                            latest_reco_id = insert_ai_recommendation(
                                user_filter=user_filter_label,
                                period_start=period_start,
                                period_end=period_end,
                                model=OLLAMA_MODEL,
                                summary_text=summary_text,
                                recommendation_text=ai_text,
                            )
                        else:
                            ai_text = latest["recommendation_text"]
                            summary_text = latest["summary_text"]
                            period_start = latest["period_start"]
                            period_end = latest["period_end"]

                        # KPIs for the report
                        last30_total = last30["kg_co2e"].sum()
                        last30_acts = len(last30)
                        by_cat_30 = last30.groupby("category")["kg_co2e"].sum().sort_values(ascending=False)

                        report_lines = []
                        report_lines.append("# Carbon Footprint Report")
                        report_lines.append("")
                        report_lines.append(f"**Period:** {period_start} to {period_end}")
                        report_lines.append(f"**Users:** {user_filter_label}")
                        report_lines.append("")
                        report_lines.append("## Summary KPIs")
                        report_lines.append(f"- Total emissions (last 30 days): {last30_total:.2f} kg CO₂e")
                        report_lines.append(f"- Number of activities: {last30_acts:,}")
                        report_lines.append("")
                        report_lines.append("### Emissions by category (last 30 days)")
                        for cat, val in by_cat_30.items():
                            report_lines.append(f"- {cat}: {val:.2f} kg CO₂e")
                        report_lines.append("")
                        report_lines.append("## AI Recommendations")
                        report_lines.append("")
                        report_lines.append(ai_text)

                        report_md = "\n".join(report_lines)
                        title = f"Carbon footprint report ({period_start} to {period_end})"

                        report_id = insert_report(
                            user_filter=user_filter_label,
                            period_start=period_start,
                            period_end=period_end,
                            title=title,
                            report_markdown=report_md,
                        )

                        st.success(f"Report generated and saved (ID: {report_id}) ✅")
                        st.markdown(report_md)
                    except Exception as e:
                        st.error(f"Report generation failed: {e}")

# ---- Upload PDF (multi-type) ----
with tab_pdf:
    st.markdown("### Upload Document (PDF) → auto-parse → store PDF & insert activity")

    user_default = (sel_users[0] if sel_users else "u_001")
    user_id_for_pdf = st.text_input("User ID for this document", value=user_default)

    doc_type_label = st.selectbox(
        "Document type",
        options=["electricity_bill", "fuel_receipt", "waste_invoice"],
        format_func=lambda x: {
            "electricity_bill": "Electricity bill",
            "fuel_receipt": "Fuel receipt",
            "waste_invoice": "Waste invoice",
        }[x]
    )

    up_pdf = st.file_uploader("Choose a PDF", type=["pdf"])
    if up_pdf is not None:
        pdf_bytes = up_pdf.read()
        sha = hashlib.sha256(pdf_bytes).hexdigest()

        # parse depending on type
        try:
            if doc_type_label == "electricity_bill":
                parsed = parse_electricity_bill(pdf_bytes)
                quantity_default = float(parsed.get("kwh") or 0.0)
                q_label = "Total energy (kWh)"
                date_default = parsed.get("period_end") or pd.Timestamp.utcnow().date()
                excerpt = parsed.get("text_excerpt", "")
            elif doc_type_label == "fuel_receipt":
                parsed = parse_fuel_receipt(pdf_bytes)
                quantity_default = float(parsed.get("litres") or 0.0)
                q_label = "Fuel volume (litres)"
                date_default = parsed.get("date") or pd.Timestamp.utcnow().date()
                excerpt = parsed.get("text_excerpt", "")
            else:  # waste_invoice
                parsed = parse_waste_invoice(pdf_bytes)
                quantity_default = float(parsed.get("kg") or 0.0)
                q_label = "Waste mass (kg)"
                date_default = parsed.get("date") or pd.Timestamp.utcnow().date()
                excerpt = parsed.get("text_excerpt", "")
        except Exception as e:
            parsed = {}
            quantity_default = 0.0
            q_label = "Quantity"
            date_default = pd.Timestamp.utcnow().date()
            excerpt = ""
            st.warning(f"Parse attempt failed: {e}")

        with st.expander("Preview first-page text (debug)"):
            st.code(excerpt or "(no text found)")

        st.write("**Parsed fields (you can edit if needed):**")
        quantity = st.number_input(q_label, value=quantity_default, min_value=0.0, step=0.1)
        doc_date = st.date_input("Document date / end of period", value=date_default)

        colA, colB = st.columns(2)
        with colA:
            if st.button("Store PDF & Insert Activity"):
                try:
                    if not blob_service:
                        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not configured on server.")
                    # 1) upload blob and get SAS
                    blob_path, sas_url = upload_pdf_and_get_sas(user_id_for_pdf, up_pdf.name, pdf_bytes)

                    # 2) insert source_documents
                    doc_id = insert_source_document(
                        user_id=user_id_for_pdf,
                        doc_type=doc_type_label,
                        period_start=None,
                        period_end=doc_date,
                        storage_url=sas_url,
                        storage_path=blob_path,
                        sha256=sha,
                        parsed_json={
                            "doc_type": doc_type_label,
                            "parsed": parsed,
                            "quantity_used": quantity,
                            "doc_date": str(doc_date),
                        }
                    )

                    # 3) map to activity row
                    if doc_type_label == "electricity_bill":
                        category = "electricity"
                        activity = "grid_kwh"
                        unit = "kwh"
                    elif doc_type_label == "fuel_receipt":
                        category = "transport"
                        fuel_type = parsed.get("fuel_type") or "petrol"
                        activity = "diesel_litre" if fuel_type == "diesel" else "petrol_litre"
                        unit = "litre"
                    else:  # waste_invoice
                        category = "waste"
                        activity = "waste_mixed_kg"
                        unit = "kg"

                    row = {
                        "user_id": user_id_for_pdf,
                        "date": doc_date,
                        "category": category,
                        "activity": activity,
                        "unit": unit,
                        "quantity": float(quantity),
                        "source_system": "pdf_" + doc_type_label,
                        "ts_ingested": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
                        "source_doc_id": str(doc_id)
                    }
                    insert_activity(row)
                    st.success("PDF stored and activity inserted ✅")
                    load_footprint.clear()

                except Exception as e:
                    st.error(f"Ingestion failed: {e}")

        with colB:
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


# ---- Reports tab ----
with tab_reports:
    st.markdown("### Saved Reports")

    with _conn() as c:
        reports_df = pd.read_sql("""
            SELECT TOP 50 report_id, user_filter, period_start, period_end, title, created_utc
            FROM dbo.reports
            ORDER BY created_utc DESC
        """, c)

    if reports_df.empty:
        st.info("No reports saved yet. Generate one from the Recommendations tab.")
    else:
        st.dataframe(reports_df, use_container_width=True, height=300)

        ids = reports_df["report_id"].tolist()
        labels = [f"{rid} - {t}" for rid, t in zip(reports_df["report_id"], reports_df["title"])]
        sel = st.selectbox("Select a report to view", options=list(zip(ids, labels)), format_func=lambda x: x[1])

        sel_id = sel[0] if isinstance(sel, tuple) else sel

        with _conn() as c:
            full = pd.read_sql(
                "SELECT report_id, title, report_markdown, created_utc FROM dbo.reports WHERE report_id = %(rid)s",
                c,
                params={"rid": sel_id},
            ).iloc[0]

        st.markdown(f"#### {full['title']}")
        st.caption(f"Created at: {full['created_utc']}")
        st.markdown(full["report_markdown"])

        st.download_button(
            "⬇️ Download report (Markdown)",
            data=full["report_markdown"].encode("utf-8"),
            file_name=f"carbon_report_{full['report_id']}.md",
            mime="text/markdown",
        )

