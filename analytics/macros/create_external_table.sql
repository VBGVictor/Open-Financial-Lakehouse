{% macro create_bronze_table() %}
    {% set query %}
        -- Criamos a tabela apontando para o local físico
        -- O dbt-spark resolve caminhos relativos à raiz do projeto dbt
        CREATE TABLE IF NOT EXISTS default.stock_prices_bronze 
        USING DELTA 
        LOCATION 'lakehouse_data/bronze/main_stock_prices';
    {% endset %}
    
    {% do run_query(query) %}
{% endmacro %}