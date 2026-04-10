import os
import time
from pyspark.sql import SparkSession

# 1. Inicializa a sessão Spark com suporte a Delta
spark = SparkSession.builder \
    .appName("DecisionEngine") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0") \
    .getOrCreate()

# 2. Caminho da Gold (ajustado para a subpasta do dbt)
gold_path = "/home/victor_goveia/projects/open_financial_lakehouse/data/gold/fct_performance_ivvb11"

# 3. Lê os dados e define o 'latest' ANTES de usar
df_gold = spark.read.format("delta").load(gold_path).orderBy("data_referencia", ascending=False)
latest = df_gold.limit(1).collect()[0]

# 4. Mapeamento das variáveis para a estratégia
data_obs = latest.data_referencia
preco = latest.preco_fechamento
variacao = latest.variacao_diaria_pct
volume = latest.volume_negociado
media_vol_21 = latest.avg_volume_21d
mme_21 = latest.mme_21d

# 5. Lógica de Decisão
print(f"\n--- ANÁLISE IVVB11 ({data_obs}) ---")

if variacao < 0 and volume < media_vol_21 and preco > mme_21:
    sinal = "COMPRA"
    motivo = "Variação negativa com volume baixo acima da MME21."
elif variacao > 0 and volume > media_vol_21 and preco < mme_21:
    sinal = "VENDA"
    motivo = "Variação positiva com volume alto abaixo da MME21."
else:
    sinal = "NEUTRO"
    motivo = "Sinal inconclusivo."

print(f"SINAL: {sinal} | MOTIVO: {motivo}")

# 6. Persistência da Decisão (Auditoria)
decision_log = [{
    "data_referencia": str(data_obs),
    "ativo": "IVVB11",
    "sinal": sinal,
    "preco_execucao": float(preco),
    "timestamp_decisao": time.strftime('%Y-%m-%d %H:%M:%S')
}]

df_decision = spark.createDataFrame(decision_log)
df_decision.write.format("delta").mode("append").save("/home/victor_goveia/projects/open_financial_lakehouse/data/gold/audit_decisions")

print("✅ Decisão persistida no Lakehouse.")