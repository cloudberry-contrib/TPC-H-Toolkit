set role hbench;
set search_path=:DB_SCHEMA_NAME,public;
:EXPLAIN_ANALYZE
--begin q15
-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create view revenue1 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1994-10-01'
		and l_shipdate < date '1994-10-01' + interval '3' month
	group by
		l_suppkey;


select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue1
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue1
	)
order by
	s_suppkey;

drop view revenue1;
--end q15
