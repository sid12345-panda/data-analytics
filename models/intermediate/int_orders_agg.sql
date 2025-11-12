{{ config(materialized='table') }}

select
  cust_key,
  count(*) as orders_count,
  sum(total_price) as total_order_value,
  min(order_date) as first_order_date,
  max(order_date) as last_order_date
from {{ ref('stg_orders') }}
group by cust_key
