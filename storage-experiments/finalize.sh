source ./base.sh


cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 

use tpch;


stop feed RegionFeed;
stop feed NationFeed;
stop feed SupplierFeed;
stop feed OrdersFeed;
stop feed CustomerFeed;
stop feed LineItemFeed;
stop feed PartFeed;
stop feed PartsuppFeed;

drop feed RegionFeed;
drop feed NationFeed;
drop feed SupplierFeed;
drop feed OrdersFeed;
drop feed CustomerFeed;
drop feed LineItemFeed;
drop feed PartFeed;
drop feed PartsuppFeed;

EOF

