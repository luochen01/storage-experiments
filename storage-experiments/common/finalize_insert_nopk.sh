set -o nounset                              # Treat unset variables as an error
v=$1

cat <<EOF | curl -XPOST --data-binary @- http://sensorium-23.ics.uci.edu:19002/query/service
stop feed twitter_insert_nopk_$v.TweetFeed;
drop feed twitter_insert_nopk_$v.TweetFeed;
EOF

