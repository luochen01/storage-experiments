#!/bin/bash


source ./base.sh

BASE=$BASE/query
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"
n=3

suffix=UNIFORM_SEQ
dv=twitter_validation_norepair

for sel in  0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01 0.1 0.2 0.5
do
    testdv="$dv"_"$suffix"
    out="$testdv"_"$sel"_scan
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -noindex -o $BASE/$out.csv "  2>&1 | tee "$BASE/$out.log"
done