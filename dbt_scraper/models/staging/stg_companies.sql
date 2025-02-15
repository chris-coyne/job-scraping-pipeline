{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append'
) }}