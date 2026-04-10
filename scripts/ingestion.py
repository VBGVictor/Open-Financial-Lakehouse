import yfinance as yf
import os
import pandas as pd
from datetime import datetime

def fetch_market_data(tickers):
    """
    Captura dados históricos da B3 e salva na camada Bronze (Raw).
    """
    bronze_path = "data/bronze/stock_prices"
    
    if not os.path.exists(bronze_path):
        os.makedirs(bronze_path)
        print(f"📁 Pasta criada: {bronze_path}")

    for ticker in tickers:
        print(f"🔍 Capturando dados de {ticker}...")
        
        # Baixando 2 anos de histórico diário
        df = yf.download(ticker, period="2y", interval="1d")
        
        if df.empty:
            print(f"⚠️ Nenhum dado encontrado para {ticker}")
            continue
            
        # Resetando o index para que a 'Date' vire uma coluna
        df.reset_index(inplace=True)
        
        # Nome do arquivo com timestamp para controle de versão manual
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        file_name = f"{bronze_path}/{ticker.replace('.SA', '')}_{timestamp}.csv"
        
        # Salvando em CSV (Raw Data)
        df.to_csv(file_name, index=False)
        print(f"✅ {ticker} salvo em: {file_name}")

if __name__ == "__main__":
    # Lista de ativos para o seu Lakehouse Financeiro
    assets = ["PETR4.SA", "IVVB11.SA", "VALE3.SA"]
    fetch_market_data(assets)