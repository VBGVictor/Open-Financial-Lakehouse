{{ config(
    materialized='table',
    file_format='delta',
    location_root='/home/victor_goveia/projects/open_financial_lakehouse/data/gold'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('stg_ivvb11') }}
),

calculo_base AS (
    SELECT
        data_referencia,
        preco_fechamento,
        volume_negociado,
        -- Mantendo sua lógica original do LAG
        LAG(preco_fechamento) OVER (ORDER BY data_referencia) as preco_anterior
    FROM silver_data
)

SELECT
    data_referencia,
    preco_fechamento,
    volume_negociado,
    -- 1. Sua métrica original: Variação Diária
    COALESCE(
        (preco_fechamento - preco_anterior) / preco_anterior * 100, 
        0
    ) as variacao_diaria_pct,
    
    -- 2. Sua métrica original: Média Móvel 5 dias
    AVG(preco_fechamento) OVER (ORDER BY data_referencia ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as media_movel_5d,

    -- 3. Nova Métrica: Média Móvel 21 dias (Preço)
    AVG(preco_fechamento) OVER (ORDER BY data_referencia ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as mme_21d,

    -- 4. Nova Métrica: Média de Volume 21 dias
    AVG(volume_negociado) OVER (ORDER BY data_referencia ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as avg_volume_21d

FROM calculo_base