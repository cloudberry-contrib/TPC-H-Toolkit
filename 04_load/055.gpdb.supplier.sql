INSERT INTO :DB_SCHEMA_NAME.supplier 
(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, 
            s_comment)
SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, 
            s_comment 
FROM :DB_EXT_SCHEMA_NAME.supplier;
