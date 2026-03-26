#!/bin/bash
set -e

# Inicia o display virtual (necessário para o Chrome mesmo em headless=new)
Xvfb :99 -screen 0 1280x1024x24 -ac &
sleep 1

# Inicia o Streamlit na porta 8571
exec streamlit run bot_streamlit.py \
    --server.port=8571 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false