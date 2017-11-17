#/bin/bash!
RESULT_DIR=~/Desktop/results
PORT=10001

DURATION=10
PERIOD=2
MAIN="java -jar target/storage-experiments-0.0.1-SNAPSHOT-jar-with-dependencies.jar "
CREATE="./create.sh"
for update in 0.0 0.05 0.1 0.25 0.5
  do
  	echo "update ratio $update"
  	$CREATE
  	$MAIN -u localhost -p 10001 -l $RESULT_DIR/log-$update -period $PERIOD -d $DURATION -update $update
  done
