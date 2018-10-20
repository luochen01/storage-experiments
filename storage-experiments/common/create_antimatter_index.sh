set -o nounset                              # Treat unset variables as an error

set_repair(){
 if(( $1>=$2 ));
 then eval "$3+=' ;'"
 else eval "$3=''"
 fi;
}

v=$1
index1="create index sid_idx if not exists on ds_tweet(sid) type btree "
index2="create index latitute_idx if not exists on ds_tweet(latitude) type btree "
index3="create index created_at_idx if not exists on ds_tweet(created_at) type btree "
index4="create index friends_idx if not exists on ds_tweet(user.friends_count) type btree "
index5="create index follow_idx if not exists on ds_tweet(user.followers_count) type btree "
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

cat <<EOF | curl -XPOST --data-binary @- http://sensorium-23.ics.uci.edu:19002/query/service 
drop dataverse twitter_antimatter_index_$v if exists;
create dataverse twitter_antimatter_index_$v if not exists;
use  twitter_antimatter_index_$v;
        create type TwitterUser if not exists as open{
            screen_name: string,
            language: string,
            friends_count: int32,
            status_count: int32,
            name: string,
            followers_count: int32
        };
        create type Tweet if not exists as open{
            id: int,
            sid: int,
            user: TwitterUser,
            latitude:double,
            longitude:double,
            created_at:datetime,
            message_text:string
        };
create dataset ds_tweet(Tweet) if not exists primary key id 
primary index strategy override
secondary index strategy antimatter
with filter on created_at
with {
  "merge-policy": {
    "name": "prefix",
    "parameters": { "max-mergable-component-size": 1073741824, "max-tolerance-component-count": 5 }
  }
};
create primary index ds_tweet_primary_idx on ds_tweet;
$index1
$index2
$index3
$index4
$index5
create feed TweetFeed with {
  "adapter-name" : "socket",
  "sockets" : "red:10001",
  "address-type" : "nc",
  "type-name" : "Tweet",
  "format" : "adm",
  "insert-feed" : "false"
};

connect feed TweetFeed to dataset ds_tweet;
start feed TweetFeed;

EOF

