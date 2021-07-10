#!/bin/bash


source base.sh

for ((i=1 ; i<=$1 ; i++ ));
do
  $QUERYMAIN -Dexec.args="$QUERYGLOBAL -output query-compact-output-4-$i.log -result query-compact-result-4-$i.log" 2>&1| tee $DIR/query-compact-4-$i.log
done


