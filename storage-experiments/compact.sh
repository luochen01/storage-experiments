source ./base.sh
set -o nounset                              # Treat unset variables as an error


cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 

compact dataset tpch.Nation;
compact dataset tpch.Customer;
compact dataset tpch.LineItem;
compact dataset tpch.Orders;
compact dataset tpch.Part;
compact dataset tpch.Partsupp;
compact dataset tpch.Region;
compact dataset tpch.Supplier;

EOF


