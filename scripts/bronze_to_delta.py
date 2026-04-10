from pyspark.sql import SparkSession
from delta import *
import os

builder = SparkSession.builder.appName("BronzeToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def convert_csv_to_delta():
    # Onde os CSVs estão (No Windows)
    csv_path = "/mnt/c/Users/Victor Goveia/Downloads/Pessoal/Projetos/Open Financial Lakehouse/data/bronze/stock_prices"
    
    # Onde vamos salvar (No Linux - SEM ERRO DE PERMISSÃO)
    delta_path = "/home/victor_goveia/lakehouse_data/bronze/delta_tables"

    if not os.path.exists(delta_path):
        os.makedirs(delta_path)

    files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]

    for file in files:
        ticker = file.split('_')[0]
        print(f"📦 Processando {ticker}...")
        
        # Lê do Windows
        df = spark.read.csv(f"{csv_path}/{file}", header=True, inferSchema=True)
        
        # Grava no Linux (Ext4)
        df.write.format("delta").mode("overwrite").save(f"{delta_path}/{ticker}")

    print(f"✅ Sucesso! Dados gravados em: {delta_path}")

if __name__ == "__main__":
    convert_csv_to_delta()