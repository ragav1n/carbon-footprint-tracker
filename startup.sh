#!/usr/bin/env bash
set -e
# Bind Streamlit to 0.0.0.0 and the PORT App Service provides (default 8000)
export PORT=${PORT:-8000}
python -m streamlit run app.py --server.address=0.0.0.0 --server.port=$PORT

