#!/bin/bash

source ./base.sh
BASE=$BASE/query
MAIN="mvn exec:java -Dexec.mainClass=edu.uci.asterixdb.storage.experiments.lsm.SecondaryIndexExperiment"
n=5

direct=true
suffix=UNIFORM_1
dv=twitter_validation_norepair

for sel in 0.00001 0.000025 0.00005 0.0001 0.00025 0.0005 0.001 0.01 0.1 0.2
do
    testdv="$dv"_"$suffix"
    out="$testdv"_"$sel"_baseline
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -skippk -o $BASE/$out.csv -nobf -nobtree -b 0"  2>&1 | tee "$BASE/$out.log"
    
    out="$testdv"_"$sel"_batch
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -skippk -o $BASE/$out.csv -nobf -nobtree -b 16384"  2>&1 | tee "$BASE/$out.log"
    

  out="$testdv"_"$sel"_batch_btree
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -skippk -o $BASE/$out.csv -nobf -b 16384"  2>&1 | tee "$BASE/$out.log"

  out="$testdv"_"$sel"_batch_btree_bf
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -skippk -o $BASE/$out.csv -b 16384"  2>&1 | tee "$BASE/$out.log"
  
  out="$testdv"_"$sel"_batch_btree_bf_id
    echo "producing $out"
    $MAIN -Dexec.args="-dv $testdv -s $sel -n $n -skippk -o $BASE/$out.csv -b 16384 -forceid "  2>&1 | tee "$BASE/$out.log"
done



