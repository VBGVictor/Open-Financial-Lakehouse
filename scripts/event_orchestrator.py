import subprocess
import time
import os

def run_pipeline():
    print(f"\n--- [EVENT] Novo fechamento de mercado detectado! ---")
    
    # 1. Aciona a extração (Bronze) - Usando o seu script de Ingestão
    print("Iniciando extração via YFinance (Bronze)...")
    subprocess.run(["python3", "scripts/ingestion.py"], check=True) 
    
    # 2. Aciona o dbt (Silver & Gold)
    print("Refinando dados no Lakehouse via dbt...")
    os.chdir("analytics")
    subprocess.run(["dbt", "run"], check=True)
    os.chdir("..")
    
    # 3. Aciona o Motor de Decisão (Intelligence)
    print("Consultando o Motor de Decisão...")
    subprocess.run(["python3", "scripts/decision_engine.py"], check=True)

if __name__ == "__main__":
    # Aqui simularíamos um sensor de horário de fechamento (18:00)
    # Por agora, ele roda uma vez e valida a integração
    run_pipeline()