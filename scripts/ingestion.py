import yfinance as yf
import os
import pandas as pd
from datetime import datetime

def fetch_market_data(tickers):
    """
    Captura dados históricos e salva na camada Bronze com particionamento por ticker.
    """
    base_bronze_path = "data/bronze/stock_prices"

    for ticker in tickers:
        print(f"🔍 Capturando dados de {ticker}...")
        
        # Criando subpasta específica para o ativo (Ex: data/bronze/stock_prices/PETR4)
        ticker_name = ticker.replace('.SA', '')
        ticker_path = os.path.join(base_bronze_path, ticker_name)
        
        if not os.path.exists(ticker_path):
            os.makedirs(ticker_path)
            print(f"📁 Pasta criada para o ativo: {ticker_path}")

        # Baixando 2 anos de histórico diário
        df = yf.download(ticker, period="2y", interval="1d")
        
        if df.empty:
            print(f"⚠️ Nenhum dado encontrado para {ticker}")
            continue
            
        # Resetando o index para que a 'Date' vire uma coluna
        df.reset_index(inplace=True)
        
        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        file_name = f"{ticker_path}/{ticker_name}_{timestamp}.csv"
        
        # Salvando em CSV
        df.to_csv(file_name, index=False)
        print(f"✅ {ticker} salvo em: {file_name}")

if __name__ == "__main__":
    # Agora você pode expandir essa lista à vontade para a Sprint 6
    assets = ["PETR4.SA", "IVVB11.SA", "VALE3.SA", "WEGE3.SA", "ITUB4.SA"]
    fetch_market_data(assets)