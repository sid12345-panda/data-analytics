with cte as
(select o_custkey , count(1)
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
where o_orderstatus = 'O'
group by o_custkey)
select *
from cte