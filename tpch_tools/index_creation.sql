create index id_LINEITEM_L_RETURNFLAG on LINEITEM using brin (L_RETURNFLAG);
create index id_LINEITEM_L_QUANTITY on LINEITEM using brin (L_QUANTITY);
create index id_LINEITEM_L_SHIPMODE on LINEITEM using brin (L_SHIPMODE);
create index id_LINEITEM_L_SHIPDATE on LINEITEM using brin (L_SHIPDATE);
create index id_CUSTOMER_C_MKTSEGMENT on LINEITEM using brin (C_MKTSEGMENT);
create index id_ORDERS_O_ORDERDATE on ORDERS using brin (O_ORDERDATE);
create index id_ORDERS_O_COMMENT on ORDERS using GIN (O_COMMENT);