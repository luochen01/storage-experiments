#!/bin/bash

BASE=/home/cluo8/experiment/lsm/query
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"
n=10

for suffix in UNIFORM_1 UNIFORM_5
do
for sel in 0.00001 0.000025 0.00005 0.0001 0.0005 0.001 0.01 0.1 0.2 0.5
do
    for dv in twitter_antimatter
    do
    cleandv=""
    if [ $suffix == "UNIFORM_1" ]; then
	cleandv="UNIFORM_5"
    else
	cleandv="UNIFORM_1"
    fi
    testdv="$dv"_"$suffix"
    cleandv="$dv"_"$cleandv"
    out="$testdv"_"$sel"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n  -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
  done
done
done
