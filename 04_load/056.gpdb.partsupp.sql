INSERT INTO :DB_SCHEMA_NAME.partsupp 
(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment 
FROM :DB_EXT_SCHEMA_NAME.partsupp;
