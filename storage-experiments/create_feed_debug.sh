source ./base.sh



for ((i=1 ; i<=$1 ; i++ ));
do
cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 
use tpch;

create feed LineItemFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "$i:10006",
    "address-type" : "nc",
    "type-name" : "LineItemType",
    "format" : "delimited-text",
    "delimiter": "|",
    "insert-feed" : "true",
    "flowcontrol.enabled": "true"
};

connect feed LineItemFeed$i to dataset LineItem;
start feed LineItemFeed$i;

EOF
done
