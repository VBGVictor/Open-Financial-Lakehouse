from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder.appName("ValidateDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Usamos 'file://' para garantir que o Spark entenda que é um caminho local absoluto
path = "file:///home/victor_goveia/lakehouse_data/bronze/delta_tables/IVVB11"

print(f"🚀 Lendo dados do Filesystem Nativo: {path}")

try:
    df = spark.read.format("delta").load(path)
    print("✅ SUCESSO ABSOLUTO! O Spark leu a tabela.")
    df.show(5)
except Exception as e:
    print(f"❌ Erro: {e}")