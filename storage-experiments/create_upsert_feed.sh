source ./base.sh
set -o nounset                              # Treat unset variables as an error
dv=$1

for i in {1..1}
do
  cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 
    use $dv;
    create feed TweetFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "asterix_nc1:1000$i",
    "address-type" : "nc",
    "type-name" : "Tweet",
    "format" : "json",
    "insert-feed" : "false"
  };
  connect feed TweetFeed$i to dataset ds_tweet;
  start feed TweetFeed$i;
EOF
done

