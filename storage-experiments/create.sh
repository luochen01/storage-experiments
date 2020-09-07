source ./base.sh
set -o nounset                              # Treat unset variables as an error


cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 

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

EOF

./create_feed.sh $1

