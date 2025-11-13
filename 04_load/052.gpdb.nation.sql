INSERT INTO :DB_SCHEMA_NAME.nation 
(n_nationkey, n_name, n_regionkey, n_comment)
SELECT n_nationkey, n_name, n_regionkey, n_comment
FROM :DB_EXT_SCHEMA_NAME.nation;
