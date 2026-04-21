from pyspark.sql import SparkSession

# O PULO DO GATO: Adicionamos o .config do pacote delta aqui para ele baixar as ferramentas
spark = SparkSession.builder \
    .appName("FixMetastore") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Caminho absoluto para a pasta Bronze
path_bronze = "/home/victor_goveia/lakehouse_data/bronze/delta_tables"

print(f"--- [CONEXÃO LAKEHOUSE] ---")
print(f"Vinculando: {path_bronze}")

try:
    # Registra a tabela no catálogo para o dbt conseguir enxergar
    spark.sql(f"CREATE TABLE IF NOT EXISTS stock_prices_bronze USING DELTA LOCATION '{path_bronze}'")
    print("✅ Sucesso: Tabela 'stock_prices_bronze' registrada no Metastore!")
except Exception as e:
    print(f"❌ Erro ao registrar: {e}")

spark.stop()