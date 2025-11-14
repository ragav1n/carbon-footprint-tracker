import os, io, re, json, hashlib
import pandas as pd
from datetime import timedelta
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pymssql
import argparse
import pdfplumber

load_dotenv()

SQL_SERVER=os.getenv("SQL_SERVER"); 
SQL_DB=os.getenv("SQL_DB") 
SQL_USER=os.getenv("SQL_USER"); 
SQL_PASS=os.getenv("SQL_PASS")
AZ_CONN=os.getenv("AZURE_STORAGE_CONNECTION_STRING") 
CONTAINER=os.getenv("UPLOADS_CONTAINER","uploads")

blob_service = BlobServiceClient.from_connection_string(AZ_CONN)

def conn():
    return pymssql.connect(server=SQL_SERVER, user=SQL_USER, password=SQL_PASS,
                           database=SQL_DB, port=1433, tds_version="7.4", charset="UTF-8")

DATE_PAT=r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})'
KWH_PAT=r'(\d+(?:\.\d+)?)\s*kwh'

def parse_bill(pdf_bytes: bytes):
    text=[]
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages: text.append(p.extract_text() or "")
    txt="\n".join(text).lower()

    kwh=None
    for m in re.finditer(KWH_PAT, txt, re.IGNORECASE):
        try: kwh=max(kwh or 0.0, float(m.group(1)))
        except: pass

    dates=[]
    for m in re.finditer(DATE_PAT, txt):
        try: dates.append(pd.to_datetime(m.group(1), dayfirst=True).date())
        except: pass
    period_start=period_end=None
    if len(dates)>=2:
        period_start=min(dates[0], dates[1])
        period_end=max(dates[0], dates[1])

    return {"kwh":kwh, "period_start":period_start, "period_end":period_end}

def upload_blob_get_sas(user_id, filename, content):
    now=pd.Timestamp.utcnow()
    path=f"uploads/{user_id}/{now.year}/{now.month:02d}/{filename}"
    bc=blob_service.get_blob_client(container=CONTAINER, blob=path)
    bc.upload_blob(content, overwrite=True)
    sas=generate_blob_sas(blob_service.account_name, CONTAINER, path,
                          account_key=blob_service.credential.account_key,
                          permission=BlobSasPermissions(read=True),
                          expiry=now+timedelta(days=1))
    url=f"https://{blob_service.account_name}.blob.core.windows.net/{CONTAINER}/{path}?{sas}"
    return path, url

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--user", required=True, help="user_id for all PDFs")
    ap.add_argument("--folder", required=True, help="folder with PDFs")
    args=ap.parse_args()

    files=[f for f in os.listdir(args.folder) if f.lower().endswith(".pdf")]
    if not files:
        print("No PDFs found."); return

    with conn() as c, c.cursor() as cur:
        for fname in files:
            fpath=os.path.join(args.folder, fname)
            data=open(fpath,"rb").read()
            sha=hashlib.sha256(data).hexdigest()
            parsed=parse_bill(data)
            kwh=parsed["kwh"]; pstart=parsed["period_start"]; pend=parsed["period_end"]
            if not kwh or not pend:
                print(f"Skip (needs manual review): {fname}"); continue

            blob_path, sas_url = upload_blob_get_sas(args.user, fname, data)

            cur.execute("""
              INSERT INTO dbo.source_documents
                (user_id,doc_type,period_start,period_end,storage_url,storage_path,sha256,parsed_json,status)
              OUTPUT INSERTED.doc_id
              VALUES (%s,'electricity_bill',%s,%s,%s,%s,%s,%s,'parsed')
            """, (args.user, pstart, pend, sas_url, blob_path, sha, json.dumps({"kwh":kwh})))
            doc_id = cur.fetchone()[0]

            cur.execute("""
              INSERT INTO dbo.activity_logs
                (user_id,[date],category,activity,unit,quantity,source_system,ts_ingested,source_doc_id)
              VALUES (%s,%s,'electricity','grid_kwh','kwh',%s,'pdf_bill',SYSUTCDATETIME(),%s)
            """, (args.user, pend, float(kwh), str(doc_id)))
            c.commit()
            print(f"Ingested {fname}: {kwh} kWh, period end {pend}")

if __name__=="__main__":
    main()

