import os
import subprocess
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from deltalake import DeltaTable
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Open Financial Lakehouse API")

# --- 1. DEFINIÇÃO DINÂMICA DE DIRETÓRIOS ---
# --- 1. DEFINIÇÃO DINÂMICA DE DIRETÓRIOS ---
# Pega a pasta onde o main.py está (api/) e sobe 1 nível para a raiz do projeto
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)

# Mapeamento para chegar na Gold que o seu terminal confirmou
# Caminho: raiz/analytics/lakehouse_data/warehouse/gold_gold.db/fct_stock_performance
GOLD_PATH = os.path.join(
    BASE_DIR, 
    "analytics", 
    "lakehouse_data", 
    "warehouse", 
    "gold_gold.db", 
    "fct_stock_performance"
)

ANALYTICS_DIR = os.path.join(BASE_DIR, "analytics")
FRONTEND_PATH = os.path.join(BASE_DIR, "frontend")

# Debug para validação interna (sem caminhos fixos no código)
if not os.path.exists(GOLD_PATH):
    print(f"⚠️ Alerta: Tabela Gold não encontrada no mapeamento dinâmico.")
else:
    print(f"✅ Conexão dinâmica estabelecida com a Gold.")

# Scripts e Binários
PYTHON_EXEC = "python" 
DBT_EXEC = "dbt"
INGESTION_SCRIPT = os.path.join(BASE_DIR, "scripts", "ingestion.py")
BRONZE_SCRIPT = os.path.join(BASE_DIR, "scripts", "bronze_to_delta.py")
# Garante que o caminho aponte para a pasta 'frontend'
FRONTEND_PATH = os.path.join(BASE_DIR, "frontend")

# --- 2. CONFIGURAÇÃO DE CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 3. FUNÇÃO DE ACESSO AO DELTA LAKE (MANTIDA) ---
def get_gold_data():
    try:
        if not os.path.exists(GOLD_PATH):
            return None
        dt = DeltaTable(GOLD_PATH)
        return dt.to_pandas().replace({np.nan: None})
    except Exception as e:
        print(f"Erro: {e}")
        return None

# --- 4. ROTAS DE DADOS (BACKEND) ---
@app.get("/market-status")
def get_market_status():
    df = get_gold_data()
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="Gold vazia")
    latest_date = df['data_referencia'].max()
    df_latest = df[df['data_referencia'] == latest_date]
    return df_latest.to_dict(orient="records")

@app.post("/ingest/{ticker}")
async def run_ingestion(ticker: str):
    # Aqui você preenche com a sua lógica de subprocess que já funcionava
    try:
        subprocess.run([PYTHON_EXEC, INGESTION_SCRIPT, ticker], check=True)
        subprocess.run([PYTHON_EXEC, BRONZE_SCRIPT], check=True)
        subprocess.run([DBT_EXEC, "run"], cwd=ANALYTICS_DIR, check=True)
        return {"status": "success", "message": f"Ativo {ticker} processado!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/history/{ticker}")
def get_ticker_history(ticker: str):
    df = get_gold_data()
    if df is None:
        raise HTTPException(status_code=500, detail="Erro ao carregar dados.")
    clean_ticker = ticker.upper().replace(".SA", "")
    df_filtered = df[df['ticker'] == clean_ticker].sort_values("data_referencia")
    return df_filtered.to_dict(orient="records")

# --- 5. SERVIR O FRONTEND (SÓ AGORA O MOUNT!) ---

# Primeiro as rotas específicas para os arquivos que você já usa
@app.get("/")
async def serve_home():
    return FileResponse(os.path.join(FRONTEND_PATH, 'home.html'))

@app.get("/terminal")
async def serve_terminal():
    return FileResponse(os.path.join(FRONTEND_PATH, 'index.html'))

# POR ÚLTIMO: O mount para carregar CSS, JS e imagens
# Colocar isso no final impede que ele "atropele" o /market-status
app.mount("/", StaticFiles(directory=FRONTEND_PATH, html=True), name="frontend")