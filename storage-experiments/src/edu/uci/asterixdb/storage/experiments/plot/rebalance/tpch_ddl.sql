drop dataverse tpch if exists;
create dataverse tpch;
use tpch;

create type LineItemType as closed{
  l_orderkey: int64,
  l_partkey: int64,
  l_suppkey: int64,
  l_linenumber: int32,
  l_quantity: int32,
  l_extendedprice: double,
  l_discount: double,
  l_tax: double,
  l_returnflag: string,
  l_linestatus: string,
  l_shipdate: string,
  l_commitdate: string,
  l_receiptdate: string,
  l_shipinstruct: string,
  l_shipmode: string,
  l_comment: string
};

create type OrdersType as closed{
  o_orderkey: int64,
  o_custkey: int64,
  o_orderstatus: string,
  o_totalprice: double,
  o_orderdate: string,
  o_orderpriority: string,
  o_clerk: string,
  o_shippriority: int32,
  o_comment: string
};

create type CustomerType as closed{
  c_custkey: int64,
  c_name: string,
  c_address: string,
  c_nationkey: int32,
  c_phone: string,
  c_acctbal: double,
  c_mktsegment: string,
  c_comment: string
};

create type PartType as closed{
  p_partkey: int64, 
  p_name: string,
  p_mfgr: string,
  p_brand: string,
  p_type: string,
  p_size: int32,
  p_container: string,
  p_retailprice: double,
  p_comment: string
};

create type PartsuppType as closed{
  ps_partkey: int64,
  ps_suppkey: int64,
  ps_availqty: int32,
  ps_supplycost: double,
  ps_comment: string
};

create type SupplierType as closed{
  s_suppkey: int64,
  s_name: string,
  s_address: string,
  s_nationkey: int32,
  s_phone: string,
  s_acctbal: double,
  s_comment: string
};

create type NationType as closed{
  n_nationkey: int32,
  n_name: string,
  n_regionkey: int32,
  n_comment: string
};

create type RegionType as closed{
  r_regionkey: int32,
  r_name: string,
  r_comment: string
};

create dataset LineItem(LineItemType) primary key l_orderkey, l_linenumber;
create dataset Orders(OrdersType)      primary key o_orderkey;
create dataset Customer(CustomerType) primary key c_custkey;
create dataset Part(PartType)         primary key p_partkey;
create dataset Partsupp(PartsuppType) primary key ps_partkey, ps_suppkey;
create dataset Supplier(SupplierType) primary key s_suppkey;
create dataset Region(RegionType)     primary key r_regionkey;
create dataset Nation(NationType)     primary key n_nationkey;


create index lineItemIdx on LineItem(l_shipdate, l_partkey, l_suppkey, l_extendedprice, l_discount, l_quantity);
create index orderDateIdx on Orders(o_orderdate, o_custkey, o_shippriority, o_orderpriority);

 create feed RegionFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10001",
    "address-type" : "nc",
    "type-name" : "RegionType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

 create feed NationFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10002",
    "address-type" : "nc",
    "type-name" : "NationType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};


create feed SupplierFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10003",
    "address-type" : "nc",
    "type-name" : "SupplierType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed OrdersFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10004",
    "address-type" : "nc",
    "type-name" : "OrdersType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed CustomerFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10005",
    "address-type" : "nc",
    "type-name" : "CustomerType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed LineItemFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10006",
    "address-type" : "nc",
    "type-name" : "LineItemType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed PartFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10007",
    "address-type" : "nc",  
    "type-name" : "PartType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed PartsuppFeed with {
    "adapter-name" : "socket",
    "sockets" : "1:10008",
    "address-type" : "nc",
    "type-name" : "PartsuppType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

connect feed RegionFeed to dataset Region;
start feed RegionFeed;

connect feed NationFeed to dataset Nation;
start feed NationFeed;

connect feed SupplierFeed to dataset Supplier;
start feed SupplierFeed;

connect feed OrdersFeed to dataset Orders;
start feed OrdersFeed;

connect feed CustomerFeed to dataset Customer;
start feed CustomerFeed;

connect feed LineItemFeed to dataset LineItem;
start feed LineItemFeed;

connect feed PartFeed to dataset Part;
start feed PartFeed;

connect feed PartsuppFeed to dataset Partsupp;
start feed PartsuppFeed;

