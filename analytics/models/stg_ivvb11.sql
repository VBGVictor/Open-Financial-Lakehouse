{{ config(materialized='table', file_format='delta', location_root='/home/victor_goveia/projects/open_financial_lakehouse/data/silver') }}

WITH source_data AS (
    SELECT * FROM parquet.`file:/home/victor_goveia/projects/open_financial_lakehouse/data/bronze/delta_tables/IVVB11`
)

SELECT
    CAST(Date AS DATE) as data_referencia,
    CAST(`('Open', 'IVVB11.SA')` AS DOUBLE) as preco_abertura,
    CAST(`('High', 'IVVB11.SA')` AS DOUBLE) as preco_maximo,
    CAST(`('Low', 'IVVB11.SA')` AS DOUBLE) as preco_minimo,
    CAST(`('Close', 'IVVB11.SA')` AS DOUBLE) as preco_fechamento,
    CAST(`('Volume', 'IVVB11.SA')` AS BIGINT) as volume_negociado
FROM source_data
WHERE Date IS NOT NULL