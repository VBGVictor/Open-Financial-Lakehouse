from pyspark.sql import SparkSession

# Usamos apenas o Delta, que já sabemos que funciona
spark = SparkSession.builder.appName("ManualBronzeRegistration") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0") \
    .getOrCreate()

print("🔨 Criando estrutura de governança manual...")

# No Spark nativo, chamamos de DATABASE o que o Unity chama de SCHEMA
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Caminho do dado no seu Linux
path_ivvb11 = "/home/victor_goveia/lakehouse_data/bronze/delta_tables/IVVB11"

# Registrando a tabela no Metastore local do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.ivvb11
    USING DELTA
    LOCATION '{path_ivvb11}'
""")

print("✅ Tabela bronze.ivvb11 registrada no Metastore!")
spark.sql("SHOW TABLES IN bronze").show()