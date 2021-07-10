source ./base.sh



for ((i=1 ; i<=$1 ; i++ ));
do
cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 
use tpch;
 create feed RegionFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10001",
    "address-type" : "nc",
    "type-name" : "RegionType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

 create feed NationFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10002",
    "address-type" : "nc",
    "type-name" : "NationType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};


create feed SupplierFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10003",
    "address-type" : "nc",
    "type-name" : "SupplierType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed OrdersFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10004",
    "address-type" : "nc",
    "type-name" : "OrdersType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed CustomerFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10005",
    "address-type" : "nc",
    "type-name" : "CustomerType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed LineItemFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10006",
    "address-type" : "nc",
    "type-name" : "LineItemType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed PartFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10007",
    "address-type" : "nc",  
    "type-name" : "PartType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

create feed PartsuppFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10008",
    "address-type" : "nc",
    "type-name" : "PartsuppType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true"
};

connect feed RegionFeed$i to dataset Region;
start feed RegionFeed$i;

connect feed NationFeed$i to dataset Nation;
start feed NationFeed$i;

connect feed SupplierFeed$i to dataset Supplier;
start feed SupplierFeed$i;

connect feed OrdersFeed$i to dataset Orders;
start feed OrdersFeed$i;

connect feed CustomerFeed$i to dataset Customer;
start feed CustomerFeed$i;

connect feed LineItemFeed$i to dataset LineItem;
start feed LineItemFeed$i;

connect feed PartFeed$i to dataset Part;
start feed PartFeed$i;

connect feed PartsuppFeed$i to dataset Partsupp;
start feed PartsuppFeed$i;


EOF
done
