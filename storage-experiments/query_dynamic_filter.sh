#!/bin/bash

source ./base.sh
BASE=$BASE/query-filter
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.RandomFilterExperiment"


readahead=128
n=10
cleandv="twitter_validation_norepair_UNIFORM_5"
for suffix in UNIFORM_1 UNIFORM_5
do
for days in 1 3 7 14 30 60 180 365
do
    for dv in twitter_inplace twitter_antimatter
    do
      testdv="$dv"_"$suffix"
      out="$testdv"_"dynamic"_"$days"
      echo "producing $out"
      $MAIN -Dexec.args="-dv $testdv -ra $readahead -d $days -n $n -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
    done
     for dv in twitter_validation
    do
      testdv="$dv"_"$suffix"
      out="$testdv"_"dynamic"_"$days"
      echo "producing $out"
      $MAIN -Dexec.args="-dv $testdv -ra $readahead -d $days -n $n -o $BASE/$out.csv -cdv $cleandv -partial"  2>&1 | tee "$BASE/$out.log"
    done  
done
done
