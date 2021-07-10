#!/bin/bash


source base.sh

for ((i=3 ; i<=$1 ; i++ ));
do
  $QUERYMAIN -Dexec.args="$QUERYGLOBAL -output query-output-4-left-$i.log -result query-result-4-left-$i.log -query 20,21" 2>&1| tee $DIR/query-4-left-$i.log
done


