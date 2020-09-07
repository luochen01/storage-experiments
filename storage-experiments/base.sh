#/bin/bash!
BASE=/home/luochen/experiment
PORT=10001
DIR=$BASE
DURATION=7200
PERIOD=1


#100M
TOTAL=100000000
FREQ=10000000
WAIT=600



CC="localhost"


export MAVEN_OPTS="-Xmx8g -server -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 -XX:+UseG1GC -XX:MaxGCPauseMillis=500"


MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.tpch.TpchClient"

GLOBAL="-dss resource/dists.dss -conf tpch.conf -workers 2"

