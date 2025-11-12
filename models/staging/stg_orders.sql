{{ config(materialized='view') }}

select
  O_ORDERKEY       as order_key,
  O_CUSTKEY        as cust_key,
  upper(O_ORDERSTATUS) as order_status,
  O_TOTALPRICE     as total_price,     -- no need to cast; already NUMBER(12,2)
  to_date(O_ORDERDATE) as order_date,
  O_ORDERPRIORITY  as order_priority,
  O_CLERK          as clerk,
  O_SHIPPRIORITY   as ship_priority
from {{ source('snowflake_sample','ORDERS') }}
