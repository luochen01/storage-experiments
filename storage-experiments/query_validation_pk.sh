#!/bin/bash

source ./base.sh

BASE=$BASE/query
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"
ns=(200 100 50 20 15 10 5 5)

for suffix in UNIFORM_1 UNIFORM_5
do
i=0
for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01
do
  n=${ns[$i]}
  for direct in false
  do
    for dv in twitter_validation twitter_validation_norepair
    do
    cleandv=""
    if [ $suffix == "UNIFORM_1" ]; then
	cleandv="UNIFORM_5"
    else
	cleandv="UNIFORM_1"
    fi
    testdv="$dv"_"$suffix"
    cleandv="$dv"_"$cleandv"
    out="$testdv"_"$sel"_"$direct"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
    done
  done
  i=$((i+1))
done
done
