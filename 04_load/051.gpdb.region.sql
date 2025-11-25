INSERT INTO :DB_SCHEMA_NAME.region 
(r_regionkey, r_name, r_comment)
SELECT r_regionkey, r_name, r_comment 
FROM :DB_EXT_SCHEMA_NAME.region;
