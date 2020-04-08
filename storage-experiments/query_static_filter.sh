#!/bin/bash

source ./base.sh

BASE=$BASE/query-filter
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.StaticFilterExperiment"

readahead=4096
n=3
cleandv="twitter_validation_norepair_UNIFORM_SEQ"
for suffix in UNIFORM_1 UNIFORM_5
do
for days in 1 3 7 14 30 60 180 365
do
    mode=RECENT  
    for dv in twitter_antimatter twitter_validation twitter_inplace
    do
      testdv="$dv"_"$suffix"
      out="$testdv"_"$mode"_"$days"_"4M"
      echo "producing $out"
      $MAIN -Dexec.args="-dv $testdv -ra $readahead -d $days -m $mode -n $n -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
    done
   
    mode=HISTORY
    for dv in twitter_antimatter twitter_validation twitter_inplace
    do
      testdv="$dv"_"$suffix"
      out="$testdv"_"$mode"_"$days"_"4M"
      echo "producing $out"
      $MAIN -Dexec.args="-dv $testdv -ra $readahead -d $days -m $mode -n $n -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
    done
    
    mode=HISTORY_PARTIAL
   for dv in twitter_antimatter twitter_validation twitter_inplace
    do
      testdv="$dv"_"$suffix"
      out="$testdv"_"$mode"_"$days"
      echo "producing $out"
      $MAIN -Dexec.args="-dv $testdv -ra $readahead -d $days -m $mode -n $n -o $BASE/$out.csv -cdv $cleandv"  2>&1 | tee "$BASE/$out.log"
    done
done
done



