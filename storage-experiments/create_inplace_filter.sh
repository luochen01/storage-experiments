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
    "name": "prefix",
    "parameters": { 
		"max-mergable-component-size":1073741824,
		"max-tolerance-component-count":5
      }
  }
};
create index sid_idx if not exists on ds_tweet(sid) type btree;
EOF

./create_upsert_feed.sh $dv
