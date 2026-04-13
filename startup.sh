#!/bin/bash
# Aponta para as dependências pré-instaladas pelo pipeline (sem pip install no container)
export PYTHONPATH="/home/site/wwwroot/.python_packages/lib/site-packages:$PYTHONPATH"

python -m streamlit run acoes.py \
  --server.port 8000 \
  --server.address 0.0.0.0 \
  --server.enableCORS false \
  --server.enableXsrfProtection false
