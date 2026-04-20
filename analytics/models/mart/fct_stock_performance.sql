{{ config(
    materialized='table',
    file_format='delta',
    location_root='/home/victor_goveia/projects/open_financial_lakehouse/data/gold'
) }}

WITH all_stocks AS (
    -- União de todos os ativos da camada Silver
    SELECT * FROM {{ ref('stg_itub4') }} UNION ALL
    SELECT * FROM {{ ref('stg_ivvb11') }} UNION ALL
    SELECT * FROM {{ ref('stg_petr4') }} UNION ALL
    SELECT * FROM {{ ref('stg_vale3') }}
),

calculo_base AS (
    SELECT
        data_referencia,
        ticker,
        preco_fechamento,
        volume_negociado,
        -- Diferença de preço para o cálculo de ganhos/perdas do RSI
        preco_fechamento - LAG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia) as diff_preco,
        LAG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia) as preco_anterior
    FROM all_stocks
),

ganhos_perdas AS (
    SELECT
        *,
        CASE WHEN diff_preco > 0 THEN diff_preco ELSE 0 END as ganho,
        CASE WHEN diff_preco < 0 THEN ABS(diff_preco) ELSE 0 END as perda
    FROM calculo_base
),

medias_finais AS (
    SELECT
        *,
        -- Médias para o RSI (14 períodos)
        AVG(ganho) OVER (PARTITION BY ticker ORDER BY data_referencia ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_ganho,
        AVG(perda) OVER (PARTITION BY ticker ORDER BY data_referencia ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_perda,
        
        -- Média Móvel Simples e Desvio Padrão para Bollinger (20 períodos)
        AVG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as mms_20d,
        STDDEV(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as desvio_padrao_20d
    FROM ganhos_perdas
),

indicadores_calculados AS (
    SELECT
        data_referencia,
        ticker,
        preco_fechamento,
        volume_negociado,
        -- 1. Variação Percentual Diária
        COALESCE((preco_fechamento - preco_anterior) / preco_anterior * 100, 0) as variacao_diaria_pct,
        
        -- 2. Média Móvel Exponencial (Sua MME21 original)
        AVG(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_referencia ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as mme_21d,
        
        -- 3. RSI 14 dias
        CASE 
            WHEN avg_perda = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_ganho / avg_perda)))
        END as rsi_14d,

        -- 4. Bandas de Bollinger
        mms_20d as bband_media,
        mms_20d + (2 * desvio_padrao_20d) as bband_superior,
        mms_20d - (2 * desvio_padrao_20d) as bband_inferior

    FROM medias_finais
)

-- SELECT FINAL: Decision Engine (Tarefa 7.3)
SELECT
    *,
    CASE 
        WHEN preco_fechamento < bband_inferior AND rsi_14d < 35 THEN 'FORTE COMPRA'
        WHEN preco_fechamento < bband_inferior THEN 'COMPRA'
        WHEN preco_fechamento > bband_superior AND rsi_14d > 65 THEN 'FORTE VENDA'
        WHEN preco_fechamento > bband_superior THEN 'VENDA'
        ELSE 'NEUTRO'
    END as sinal_estrategia
FROM indicadores_calculados