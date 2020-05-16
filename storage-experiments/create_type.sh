source ./base.sh
set -o nounset                              # Treat unset variables as an error
dv=$1
cat <<EOF | curl --data-urlencode statement@- http://$CC:19002/query/service 
drop dataverse $dv if exists;
create dataverse $dv if not exists;
use $dv;
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
EOF

