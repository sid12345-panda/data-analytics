{{ config(materialized='table') }}

select
  i.cust_key as customer_id,
  i.orders_count,
  i.total_order_value,
  c.C_NAME as customer_name
from {{ ref('int_orders_agg') }} i
left join {{ source('snowflake_sample','CUSTOMER') }} c
  on i.cust_key = c.C_CUSTKEY
