set role hbench;
set search_path=:DB_SCHEMA_NAME,public;
:EXPLAIN_ANALYZE
--begin q14
-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998


select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1996-04-01'
	and l_shipdate < date '1996-04-01' + interval '1' month;
--end q14
