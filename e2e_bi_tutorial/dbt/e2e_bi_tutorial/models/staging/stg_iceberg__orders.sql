{{
  config(
    on_schema_change='sync_all_columns',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    properties={
      "format": "'PARQUET'",
      "sorted_by": "ARRAY['order_id']",
    }
  )
}}

with source as (
  select *, row_number() over (partition by id order by id desc) as row_num
  from {{ source('stg_iceberg', 'orders') }}
),

deduped_and_renamed as (
  select
    CAST(id AS VARCHAR) AS order_id,
    DATE_PARSE(created_at, '%Y-%m-%dT%H:%i:%s') AS order_created_at,
    CAST(qty AS DECIMAL) AS qty,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(store_id AS VARCHAR) AS store_id
  from source
  where row_num = 1
)

select * from deduped_and_renamed