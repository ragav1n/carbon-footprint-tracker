import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

# ---- EDIT THESE ----
SQL_SERVER = "carbon-sql-server.database.windows.net"   # your server
SQL_DB     = "carbon_tracker_db"
SQL_USER   = "sqladminuser"
SQL_PASS   = "Ragava@2005"
# --------------------

# Use pymssql (FreeTDS). Tip: tds_version 7.4 works well with Azure SQL.
conn_url = URL.create(
    "mssql+pymssql",
    username=SQL_USER,
    password=SQL_PASS,
    host=SQL_SERVER,
    port=1433,
    database=SQL_DB,
    query={
        "charset": "UTF-8",
        "tds_version": "7.4",
        # pymssql enables TLS automatically against Azure SQL; these are redundant for most setups:
        # "sslmode": "require"
    },
)

engine = sa.create_engine(conn_url)  # <-- no fast_executemany with pymssql

# Files in the same directory
ACTIVITY_CSV = "activity_logs_sample.csv"
FACTORS_CSV  = "emission_factors_sample.csv"

# Load activity
df_act = pd.read_csv(ACTIVITY_CSV)
df_act["date"] = pd.to_datetime(df_act["date"]).dt.date

with engine.begin() as conn:
    # faster inserts: chunks + multi-row
    df_act.to_sql("activity_logs", conn, schema="dbo",
                  if_exists="append", index=False, method="multi", chunksize=1000)

# Load factors
df_f = pd.read_csv(FACTORS_CSV)
with engine.begin() as conn:
    df_f.to_sql("emission_factors", conn, schema="dbo",
                if_exists="append", index=False, method="multi", chunksize=1000)

with engine.begin() as conn:
    cnt_a = conn.execute(sa.text("SELECT COUNT(*) FROM dbo.activity_logs")).scalar()
    cnt_f = conn.execute(sa.text("SELECT COUNT(*) FROM dbo.emission_factors")).scalar()
    print("activity_logs rows:", cnt_a)
    print("emission_factors rows:", cnt_f)

