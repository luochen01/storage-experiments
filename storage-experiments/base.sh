#/bin/bash!
BASE=/home/luochen/experiment
PORT=10001

DURATION=7200
PERIOD=1


#100M
TOTAL=100000000
FREQ=10000000
WAIT=600


SORT="64MB"

NC="localhost"
CC="localhost"


REPAIR_PORT=10001

MAIN="java -Xmx1024m -jar target/storage-experiments-0.0.1-SNAPSHOT-jar-with-dependencies.jar -u $NC -p $PORT -period $PERIOD -m RANDOM"
REPAIR="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.feed.FeedRepairDriver"

