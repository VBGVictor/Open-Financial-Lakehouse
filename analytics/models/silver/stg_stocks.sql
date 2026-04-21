{{ config(materialized='table') }}

{% set create_cmd %}
  CREATE OR REPLACE TEMPORARY VIEW raw_bronze_view
  USING delta
  OPTIONS (path "lakehouse_data/bronze/main_stock_prices")
{% endset %}

{% do run_query(create_cmd) %}

SELECT 
    ticker,
    CAST(Date AS DATE) as data_referencia,
    CAST(Open AS DOUBLE) as preco_abertura,
    CAST(High AS DOUBLE) as preco_maximo,
    CAST(Low AS DOUBLE) as preco_minimo,
    CAST(Close AS DOUBLE) as preco_fechamento,
    CAST(Volume AS DOUBLE) as volume
FROM raw_bronze_view
-- O PULO DO GATO: Removemos cabeçalhos duplicados ou linhas com texto onde deveria ser número
WHERE Date IS NOT NULL 
  AND try_cast(Close AS DOUBLE) IS NOT NULL