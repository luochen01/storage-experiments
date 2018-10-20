set -o nounset                              # Treat unset variables as an error
v=$1

cat <<EOF | curl -XPOST --data-binary @- http://sensorium-23.ics.uci.edu:19002/query/service
stop feed twitter_deletebtree_index_$v.TweetFeed;
drop feed twitter_deletebtree_index_$v.TweetFeed;
EOF

