{{ config(
  materialized='incremental',
  unique_key='cust_key',
  incremental_strategy='merge',
  partition_by={'field':'_dbt_loaded_at','data_type':'timestamp'}
) }}

with new_stg as (
    select * from {{ ref('stg_orders_incr') }}
    {% if is_incremental() %}
      where _dbt_loaded_at > (select coalesce(max(_dbt_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

agg as (
    select
      cust_key,
      count(*)               as orders_count,
      sum(total_price)       as total_order_value,
      min(order_date)        as first_order_date,
      max(order_date)        as last_order_date
    from new_stg
    group by cust_key
)

select * from agg
