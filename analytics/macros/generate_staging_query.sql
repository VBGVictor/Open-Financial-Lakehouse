{% macro generate_staging_query(ticker_name) %}

WITH raw_data AS (
    -- Lendo o CSV bruto. O Spark vai atribuir _c0, _c1, _c2...
    SELECT * FROM csv.`/home/victor_goveia/projects/open_financial_lakehouse/data/bronze/stock_prices/{{ ticker_name }}`
)

SELECT
    -- Mapeamos as colunas conforme a estrutura do yfinance
    CAST(_c0 AS DATE) as data_referencia,
    CAST(_c1 AS DOUBLE) as preco_abertura,
    CAST(_c2 AS DOUBLE) as preco_maximo,
    CAST(_c3 AS DOUBLE) as preco_minimo,
    CAST(_c4 AS DOUBLE) as preco_fechamento,
    CAST(_c5 AS LONG) as volume_negociado,
    '{{ ticker_name }}' as ticker
FROM raw_data
-- Filtro essencial: remove a linha do cabeçalho original e linhas vazias
WHERE _c0 IS NOT NULL AND _c0 != 'Date'

{% endmacro %}