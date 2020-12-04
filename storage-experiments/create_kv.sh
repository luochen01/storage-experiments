set -o nounset                              # Treat unset variables as an error

url=localhost
cat <<EOF | curl --data-urlencode statement@- http://$url:19002/query/service 

drop dataverse test if exists;

create dataverse test;
use test;

create type kvType as closed{
pkey: int64,
value1:int64,
value2:int64,
value3:int64,
value4:int64,
value5:int64,
value6:int64,
value7:int64,
value8:int64,
value9:int64,
value10:int64
};

create dataset kv(kvType) primary key pkey;

EOF

for i in {1..1}
do
   for j in {1..10}
   do
      cat <<EOF | curl --data-urlencode statement@- http://$url:19002/query/service 
      use test;
      create index idx$i$j on kv(value$j);
EOF
   done
done


cat <<EOF | curl --data-urlencode statement@- http://$url:19002/query/service 
use test;
create feed kvFeed with {
    "adapter-name" : "socket",
    "sockets" : "asterix_nc1:10001",
    "address-type" : "nc",
    "type-name" : "kvType",
    "format" : "json",
    "insert-feed" : "false"
};
connect feed kvFeed to dataset kv;
start feed kvFeed;
EOF


