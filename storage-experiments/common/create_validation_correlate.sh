set -o nounset                              # Treat unset variables as an error
v=$1
merge="${2:-1073741824}"
cat <<EOF | curl -XPOST --data-binary @- http://sensorium-23.ics.uci.edu:19002/query/service 
drop dataverse twitter_validation_$v if exists;
create dataverse twitter_validation_$v if not exists;
use  twitter_validation_$v;
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
secondary index strategy validation
with filter on created_at
with {
  "merge-policy": {
    "name": "correlated-prefix",
    "parameters": { "max-mergable-component-size": $merge, "max-tolerance-component-count": 5, "correlate-on-key-index": true }
  }
};
create primary index ds_tweet_primary_idx on ds_tweet;
create index sid_idx if not exists on ds_tweet(sid) type btree repair AUTO;
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

