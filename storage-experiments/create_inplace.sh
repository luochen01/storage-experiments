source ./base.sh
set -o nounset                              # Treat unset variables as an error
v=$1
dv=twitter_inplace_$1
./create_type.sh $dv

cat <<EOF | curl -XPOST --data-binary @- http://$CC:19002/query/service 
use $dv;
create dataset ds_tweet(Tweet) if not exists primary key id 
primary index strategy inplace snapshot
secondary index strategy validation
with filter on created_at
with {
  "merge-policy": {
    "name": "concurrent",
    "parameters": {  "min-merge-component-count": 3, 
        "max-merge-component-count": 10, 
        "max-component-count": 30, 
        "size-ratio": 1.2
      }
  }
};
create index sid_idx if not exists on ds_tweet(sid) type btree;
EOF

./create_upsert_feed.sh $dv
