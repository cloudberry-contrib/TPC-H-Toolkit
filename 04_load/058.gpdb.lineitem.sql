INSERT INTO :DB_SCHEMA_NAME.lineitem 
(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, 
            l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, 
            l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, 
            l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, 
            l_receiptdate, l_shipinstruct, l_shipmode, l_comment
FROM :DB_EXT_SCHEMA_NAME.lineitem;
