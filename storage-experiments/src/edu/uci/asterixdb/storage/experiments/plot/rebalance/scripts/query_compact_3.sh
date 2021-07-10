#!/bin/bash


source base.sh

for ((i=1 ; i<=$1 ; i++ ));
do
  $QUERYMAIN -Dexec.args="$QUERYGLOBAL -output query-compact-output-3-$i.log -result query-compact-result-3-$i.log" 2>&1| tee $DIR/query-compact-3-$i.log
done


