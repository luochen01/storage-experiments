set -o nounset                              # Treat unset variables as an error
v=$1

cat <<EOF | curl -XPOST --data-binary @- http://sensorium-22.ics.uci.edu:19002/query/service
stop feed twitter_$v.TweetFeed;
drop feed twitter_$v.TweetFeed;
EOF

