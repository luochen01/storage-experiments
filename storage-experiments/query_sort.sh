#!/bin/bash

source ./base.sh

BASE=BASE/query
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"
n=2

direct=true

for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01
do
    dv=twitter_validation_norepair_UNIFORM_1 
    testdv=$dv
    batch=16384   
    out="sort"_"$dv"_"$sel"_"batch"
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -o $BASE/$out.csv -sortrecord -b $batch"  2>&1 | tee "$BASE/$out.log"
    
    batch=0
    out="sort"_"$dv"_"$sel"
    echo "producing $out"
   $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -o $BASE/$out.csv -sortrecord -b $batch"  2>&1 | tee "$BASE/$out.log"

done

