source ./base.sh


for ((i=1;i<=$1;i++));
do
cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 

use tpch;


stop feed RegionFeed$i;
stop feed NationFeed$i;
stop feed SupplierFeed$i;
stop feed OrdersFeed$i;
stop feed CustomerFeed$i;
stop feed LineItemFeed$i;
stop feed PartFeed$i;
stop feed PartsuppFeed$i;

drop feed RegionFeed$i;
drop feed NationFeed$i;
drop feed SupplierFeed$i;
drop feed OrdersFeed$i;
drop feed CustomerFeed$i;
drop feed LineItemFeed$i;
drop feed PartFeed$i;
drop feed PartsuppFeed$i;
EOF
done
