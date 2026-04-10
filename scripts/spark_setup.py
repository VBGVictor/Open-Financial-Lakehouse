from pyspark.sql import SparkSession
from delta import *

def get_spark_session():
    # Caminho para o JAR do Unity Catalog (que veio no seu download do 0.4.0)
    # Se você extraiu na pasta do projeto, o caminho será algo assim:
    uc_jar_path = "./unitycatalog-0.4.0/connectors/spark/target/unitycatalog-spark_2.12-0.4.0.jar"

    builder = SparkSession.builder \
        .appName("FinancialLakehouse") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.unity", "org.apache.spark.sql.unity.UnityCatalog") \
        .config("spark.sql.catalog.unity.uri", "http://localhost:8080") \
        .config("spark.sql.catalog.unity.token", "not-used") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Adicionando os pacotes do Delta Lake e Unity Catalog
    spark = configure_spark_with_delta_pip(builder, extra_packages=[
        "io.unitycatalog:unitycatalog-spark_2.12:0.4.0"
    ]).getOrCreate()

    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("✅ Spark Session criada com sucesso e conectada ao Unity Catalog!")
    spark.sql("SHOW CATALOGS").show()