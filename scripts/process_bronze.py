from spark_setup import get_spark_session
import os
from pathlib import Path

spark = get_spark_session()

# Descobre o caminho relativo da pasta de dados
project_root = Path(__file__).resolve().parent.parent
target_path = os.path.join(project_root, "lakehouse_data/bronze/main_stock_prices")

# O PULO DO GATO: Criamos uma tabela EXTERNA. 
# O Spark registra o nome, mas os dados ficam na pasta.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS stock_prices_bronze 
    USING DELTA 
    LOCATION '{target_path}'
""")

print(f"✅ Tabela registrada com sucesso apontando para: {target_path}")
spark.sql("SELECT ticker, count(*) FROM stock_prices_bronze GROUP BY ticker").show()