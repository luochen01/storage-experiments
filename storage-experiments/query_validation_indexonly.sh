#!/bin/bash

source ./base.sh
BASE=$BASE/query
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"

n=20
for suffix in UNIFORM_1 UNIFORM_5
do
for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01 0.1
do
    for dv in twitter_antimatter
    do
    testdv="$dv"_"$suffix"
    out="$testdv"_"$sel"_"indexonly_sort"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -o $BASE/$out.csv -indexonly -sortid"  2>&1 | tee "$BASE/$out.log"
    done
done
done

for suffix in UNIFORM_1 UNIFORM_5
do
for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01 0.1
do
    for dv in twitter_antimatter
    do
    testdv="$dv"_"$suffix"
    out="$testdv"_"$sel"_"indexonly"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -o $BASE/$out.csv -indexonly"  2>&1 | tee "$BASE/$out.log"
    done
done
done


ns=(200 100 50 20 20 20 20 10 5)
for suffix in UNIFORM_1 UNIFORM_5
do
i=0
for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01 0.1
do
   n=${ns[$i]}
   for dv in twitter_validation twitter_validation_norepair
    do
    testdv="$dv"_"$suffix"
    out="$testdv"_"$sel"_"indexonly"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -o $BASE/$out.csv -indexonly "  2>&1 | tee "$BASE/$out.log"
    done
    i=$((i+1))
done
done

