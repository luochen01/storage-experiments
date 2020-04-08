#!/bin/bash

source ./base.sh
BASE=$BASE/misc
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"

testdv=twitter_validation_norepair_UNIFORM_1
testdv=twitter_antimatter_UNIFORM_1

n=5
for sel in 0.0001 0.001 0.01 0.1
do
   for batch in 0 128 256 512 1024 4096 8092 16184
   do
     out="$testdv"_"$sel"_"$batch"
     echo producing $out
     $MAIN -Dexec.args="-dv $testdv -s $sel -n $n  -o $BASE/$out.csv -b $batch -skippk"  2>&1 | tee "$BASE/$out.log"
   done
done