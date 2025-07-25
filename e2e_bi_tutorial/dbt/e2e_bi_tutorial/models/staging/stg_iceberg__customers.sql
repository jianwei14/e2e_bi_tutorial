{{
  config(
    on_schema_change='sync_all_columns',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['traffic_source']",
      "sorted_by": "ARRAY['id']",
    }
  )
}}

with source as (
  select *, row_number() over (partition by id order by id desc) as row_num
  from {{ source('stg_iceberg', 'customers') }}
),

deduped_and_renamed as (
  select
    CAST(id AS VARCHAR) AS id,
    DATE_PARSE(created_at, '%Y-%m-%dT%H:%i:%s') AS acc_created_at,
    CAST(first_name AS VARCHAR) AS first_name,
    CAST(last_name AS VARCHAR) AS last_name,
    CAST(gender AS VARCHAR) AS gender,
    CAST(country AS VARCHAR) AS country,
    CAST(address AS VARCHAR) AS address,
    CAST(phone AS VARCHAR) AS phone,
    CAST(email AS VARCHAR) AS email,
    CAST(payment_method AS VARCHAR) AS payment_method,
    CAST(traffic_source AS VARCHAR) AS traffic_source,
    CAST(referrer AS VARCHAR) AS referrer,
    CAST(customer_age AS DECIMAL) AS customer_age,
    CAST(device_type AS VARCHAR) AS device_type
  from source
  where row_num = 1
)
 
select * from deduped_and_renamed