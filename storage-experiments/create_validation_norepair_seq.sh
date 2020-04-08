source ./base.sh
set -o nounset                              # Treat unset variables as an erro
dv=twitter_validation_norepair_seq_$1
./create_type.sh $dv
cat <<EOF | curl -XPOST --data-binary @- http://"$CC":19002/query/service 
use $dv;
create dataset ds_tweet(Tweet) if not exists primary key id 
primary index strategy override
secondary index strategy validation
with filter on created_at
with {
  "merge-policy": {
    "name": "correlated-concurrent",
    "parameters": {  "min-merge-component-count": 3, 
        "max-merge-component-count": 10, 
        "max-component-count": 30, 
        "size-ratio": 1.2,
        "correlate-on-key-index": true 
      }
  }
};
create primary index ds_tweet_primary_idx on ds_tweet;
create index sid_idx if not exists on ds_tweet(sid) type btree;
EOF

./create_upsert_feed.sh $dv