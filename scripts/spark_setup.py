from pyspark.sql import SparkSession
from delta import *

def get_spark_session():
    builder = SparkSession.builder \
        .appName("FinancialLakehouse") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.unity", "org.apache.spark.sql.unity.UnityCatalog") \
        .config("spark.sql.catalog.unity.uri", "http://localhost:8080") \
        .config("spark.sql.catalog.unity.token", "not-used") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # O comando abaixo deve estar alinhado com o 'builder' lá de cima
    spark = configure_spark_with_delta_pip(builder, extra_packages=[
        "io.delta:delta-spark_2.13:3.1.0",
        "io.unitycatalog:unitycatalog-spark_2.13:0.4.0"
    ]).getOrCreate()

    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("✅ Spark Session criada com sucesso!")
    try:
        spark.sql("SHOW CATALOGS").show()
    except Exception as e:
        print(f"⚠️ Nota: Para listar catalogos, o servidor do Unity Catalog deve estar rodando em localhost:8080. Erro: {e}")