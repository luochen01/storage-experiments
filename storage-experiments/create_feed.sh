source ./base.sh


cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 

use tpch;


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




EOF

