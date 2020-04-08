#!/bin/bash
source ./base.sh
BASE=$BASE/query

MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"
n=5

direct=true

for suffix in UNIFORM_1 UNIFORM_5
do
for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01
do
    for dv in twitter_validation twitter_validation_norepair
    do
    cleandv=""
    testdv="$dv"_"$suffix"
    out="$testdv"_"$sel"_"$direct"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -skippk -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
    done
done
done

