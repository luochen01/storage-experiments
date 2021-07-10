source ./base.sh
set -o nounset                              # Treat unset variables as an error


cat <<EOF | curl -d "profile=timings" --data-urlencode  statement@- http://54.185.203.210:19002/query/service 

select * from tpch.LineItem;

EOF

