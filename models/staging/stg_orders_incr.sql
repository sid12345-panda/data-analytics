{{ config(
  materialized='incremental',
  unique_key='ORDER_KEY',
  incremental_strategy='merge'
) }}

with src as (
    select
        O_ORDERKEY      as ORDER_KEY,
        O_CUSTKEY       as CUST_KEY,
        upper(O_ORDERSTATUS) as ORDER_STATUS,
        O_TOTALPRICE    as TOTAL_PRICE,
        to_date(O_ORDERDATE) as ORDER_DATE,
        O_ORDERPRIORITY as ORDER_PRIORITY,
        O_CLERK         as CLERK,
        O_SHIPPRIORITY  as SHIP_PRIORITY,
        current_timestamp() as _dbt_loaded_at
    from {{ source('snowflake_sample', 'ORDERS') }}

    {% if is_incremental() %}
      -- only take rows newer than the max ORDER_DATE already in the target
      where to_date(O_ORDERDATE) > (select coalesce(max(ORDER_DATE), '1900-01-01') from {{ this }})
    {% endif %}
)

select * from src
