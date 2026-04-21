{{ config(
    materialized='table',
    file_format='delta',
    location='lakehouse_data/gold/fct_stock_performance'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('stg_stocks') }}
),

calculo_performance AS (
    SELECT 
        *,
        -- Variação percentual entre o fechamento de hoje e o de ontem
        (preco_fechamento / LAG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia) - 1) * 100 as variacao_diaria_pct,
        -- Média móvel de 7 dias para suavizar a tendência no gráfico
        AVG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as media_movel_7d
    FROM silver_data
)

SELECT * FROM calculo_performance