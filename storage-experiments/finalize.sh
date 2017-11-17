set -o nounset                              # Treat unset variables as an error

# ddl to register the twitter dataset
cat <<'EOF' | curl -XPOST --data-binary @- http://localhost:19002/aql
drop feed twitter_ingest.TweetFeed;
EOF

