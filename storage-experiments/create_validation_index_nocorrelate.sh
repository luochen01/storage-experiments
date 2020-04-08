source ./base.sh
set -o nounset                              # Treat unset variables as an erro

set_repair(){
 if(( $1>=$2 ));
 then eval "$3+=' repair AUTO;'"
 else eval "$3=''"
 fi;
}

v=$1
index1="create index sid_idx1 if not exists on ds_tweet(sid) type btree "
index2="create index sid_idx2 if not exists on ds_tweet(sid) type btree "
index3="create index sid_idx3 if not exists on ds_tweet(sid) type btree "
index4="create index sid_idx4 if not exists on ds_tweet(sid) type btree "
index5="create index sid_idx5 if not exists on ds_tweet(sid) type btree "
set_repair v 1 index1
set_repair v 2 index2 
set_repair v 3 index3 
set_repair v 4 index4 
set_repair v 5 index5 
echo "index1=$index1"
echo "index2=$index2"
echo "index3=$index3"
echo "index4=$index4"
echo "index5=$index5"

dv=twitter_validation_index_nocorrelate_$1
./create_type.sh $dv

cat <<EOF | curl -XPOST --data-binary @- http://$CC:19002/query/service 
use $dv;
create dataset ds_tweet(Tweet) if not exists primary key id 
primary index strategy override
secondary index strategy validation
with filter on created_at
with {
  "merge-policy": {
    "name": "concurrent",
    "parameters": { "min-merge-component-count": 3, "max-merge-component-count": 10, "max-component-count": 30, "size-ratio": 1.2}
  }
};
create primary index ds_tweet_primary_idx on ds_tweet;
$index1
$index2
$index3
$index4
$index5
EOF

./create_upsert_feed.sh $dv


