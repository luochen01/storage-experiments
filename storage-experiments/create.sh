source ./base.sh
set -o nounset                              # Treat unset variables as an error

dv=twitter
./create_type.sh $dv

cat <<EOF | curl --data-urlencode statement@- http://localhost:19002/query/service 
use $dv;
create dataset ds_tweet(Tweet) if not exists primary key id 
with filter on created_at
with {
  "merge-policy": {
    "name": "concurrent",
    "parameters": { "min-merge-component-count": 3, "max-merge-component-count": 10, "max-component-count": 30, "size-ratio": 1.2}
  }
};
create primary index ds_tweet_primary_idx on ds_tweet;
create index sid_idx if not exists on ds_tweet(sid) type btree;
create index latitute_idx if not exists on ds_tweet(latitude) type btree;
create index created_at_idx if not exists on ds_tweet(created_at) type btree;
create index friends_idx if not exists on ds_tweet(user.friends_count) type btree;
create index follow_idx if not exists on ds_tweet(user.followers_count) type btree;

EOF

./create_upsert_feed.sh $dv

