source ./base.sh
set -o nounset                              # Treat unset variables as an error
dv=$1

for i in {1..8}
do
  cat <<EOF | curl -XPOST --data-binary @- http://"$CC":19002/query/service 
    use $dv;
    create feed TweetFeed$i with {
    "adapter-name" : "socket",
    "sockets" : "1:1000$i",
    "address-type" : "nc",
    "type-name" : "Tweet",
    "format" : "json",
    "insert-feed" : "true"
  };
  connect feed TweetFeed$i to dataset ds_tweet;
  start feed TweetFeed$i;
EOF
done

