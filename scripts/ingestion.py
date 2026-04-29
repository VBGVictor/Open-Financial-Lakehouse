import yfinance as yf
import os
import sys
import pandas as pd
from datetime import datetime

def fetch_market_data(tickers):
    """
    Captura dados históricos e salva na camada Bronze com caminho absoluto.
    """
    # Descobre a raiz do projeto dinamicamente
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir) 
    base_bronze_path = os.path.join(project_root, "data", "bronze", "stock_prices")

    for ticker in tickers:
        # Garante que o ticker tenha o .SA para o Yahoo Finance
        ticker_yf = ticker.upper() if ".SA" in ticker.upper() else f"{ticker.upper()}.SA"
        ticker_name = ticker_yf.replace('.SA', '')
        
        print(f"🔍 Capturando dados de {ticker_yf}...")
        
        ticker_path = os.path.join(base_bronze_path, ticker_name)
        if not os.path.exists(ticker_path):
            os.makedirs(ticker_path)
            print(f"📁 Nova pasta criada: {ticker_path}")

        df = yf.download(ticker_yf, period="2y", interval="1d")
        
        if df.empty:
            print(f"⚠️ Nenhum dado encontrado para {ticker_yf}")
            continue
            
        df.reset_index(inplace=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        file_name = f"{ticker_name}_{timestamp}.csv"
        final_file_path = os.path.join(ticker_path, file_name)
        
        df.to_csv(final_file_path, index=False)
        print(f"✅ Salvo em: {final_file_path}")

if __name__ == "__main__":
    # 1. Mapeamento da raiz para localizar as pastas
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    base_bronze_path = os.path.join(project_root, "data", "bronze", "stock_prices")

    # 2. Identifica os ativos que JÁ ESTÃO na pasta (Dinâmico)
    assets_to_process = []
    if os.path.exists(base_bronze_path):
        assets_to_process = os.listdir(base_bronze_path)

    # 3. Se você passar um novo ativo via terminal (ex: python ingestion.py ITSA4)
    if len(sys.argv) > 1:
        new_ticker = sys.argv[1].upper().replace(".SA", "")
        if new_ticker not in assets_to_process:
            assets_to_process.append(new_ticker)

    # 4. Executa apenas se houver algo para processar
    if assets_to_process:
        fetch_market_data(assets_to_process)
    else:
        print("📭 Nenhuma pasta de ativo encontrada e nenhum ticker informado.")