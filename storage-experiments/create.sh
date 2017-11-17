set -o nounset                              # Treat unset variables as an error

# ddl to register the twitter dataset
cat <<'EOF' | curl -XPOST --data-binary @- http://localhost:19002/aql
stop feed twitter_ingest.TweetFeed;
drop dataverse twitter_ingest if exists;
create dataverse twitter_ingest if not exists;
use dataverse twitter_ingest;
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
            user: TwitterUser,
            latitude:double,
            longitude:double,
            created_at:string,
            message_text:string
        };
create dataset ds_tweet(Tweet) if not exists primary key id 
using compaction policy prefix (("max-mergable-component-size"="1073741824"),("max-tolerance-component-count"="5"));
create index latitute_idx if not exists on ds_tweet(latitude) type btree;
create index created_at_idx if not exists on ds_tweet(created_at) type btree;
create index friends_idx if not exists on ds_tweet(user.friends_count) type btree;
create index follow_idx if not exists on ds_tweet(user.followers_count) type btree;
create index name_idx if not exists on ds_tweet(user.name) type btree;

create feed TweetFeed using socket_adapter
(
    ("sockets"="asterix_nc1:10001"),
    ("address-type"="nc"),
    ("type-name"="Tweet"),
    ("format"="adm")
);
connect feed TweetFeed to dataset ds_tweet;
start feed TweetFeed;
EOF

