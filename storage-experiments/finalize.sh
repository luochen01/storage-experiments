source ./base.sh
#set -o nounset                              # Treat unset variables as an error
dv=$1

for i in {1..4}
do
cat <<EOF | curl --data-urlencode statement@- http://$CC:19002/query/service
use  $dv;
stop feed $dv.TweetFeed$i;
drop feed $dv.TweetFeed$i;
EOF
done

if [ -n "$2" ]; then
	cat <<EOF | curl -XPOST --data-binary @- http://$CC:19002/query/service
	drop dataverse $dv if exists;
EOF
fi
