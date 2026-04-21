import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import input_file_name, regexp_extract

# --- 1. CONFIGURAÇÃO DINÂMICA DE CAMINHOS ---
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent

# Caminhos relativos à raiz do projeto
csv_path = os.path.join(project_root, "data/bronze/stock_prices/*/*.csv")
delta_path = os.path.join(project_root, "lakehouse_data/bronze/main_stock_prices")

print(f"🚀 Iniciando processamento genérico...")
print(f"📂 Raiz do projeto detectada: {project_root}")
print(f"🔍 Buscando CSVs em: {csv_path}")

# --- 2. INICIALIZAÇÃO DO SPARK ---
# Aqui ligamos o motor do Lakehouse
builder = SparkSession.builder.appName("BronzeToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --- 3. PROCESSAMENTO DOS DADOS ---
try:
    print("📦 Lendo arquivos CSV...")
    df_raw = spark.read.csv(f"file://{csv_path}", header=True, inferSchema=True)

    print("🏷️ Extraindo o ticker do caminho do arquivo...")
    # Agora o Spark saberá o que é regexp_extract e input_file_name
    df_with_ticker = df_raw.withColumn("ticker", regexp_extract(input_file_name(), r"stock_prices/([^/]+)/", 1))

    print("🔄 Consolidando ativos e gravando em formato Delta...")
    df_with_ticker.write.format("delta") \
      .mode("overwrite") \
      .partitionBy("ticker") \
      .save(f"file://{delta_path}")

    print(f"✅ Sucesso! Tabela única gravada em: {delta_path}")
    df_with_ticker.groupBy("ticker").count().show()

except Exception as e:
    print(f"❌ Erro durante o processamento: {e}")