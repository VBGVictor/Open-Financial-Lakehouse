from spark_setup import get_spark_session
import os

spark = get_spark_session()

# 1. Criar o Catálogo e Schema se não existirem (Governança)
spark.sql("CREATE CATALOG IF NOT EXISTS financial_market")
spark.sql("USE CATALOG financial_market")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("USE SCHEMA bronze")

# 2. Ler o arquivo CSV que capturamos na Tarefa 2.1
csv_path = "data/bronze/stock_prices/PETR4_*.csv" # Pega o arquivo que você gerou
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

# 3. Salvar como DELTA TABLE (Aqui vira Lakehouse de verdade)
# O caminho de destino deve ser fora da pasta do UC para ser uma External Table
target_delta_path = os.path.abspath("data/bronze/delta/petr4_raw")

print(f"📥 Gravando dados na Camada Bronze (Delta)...")
df_raw.write.format("delta").mode("overwrite").save(target_delta_path)

# 4. Registrar no Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS petr4_prices 
    USING DELTA 
    LOCATION '{target_delta_path}'
""")

print("🔥 Tabela PETR4_PRICES registrada com sucesso no Unity Catalog!")
spark.sql("SELECT * FROM petr4_prices LIMIT 5").show()