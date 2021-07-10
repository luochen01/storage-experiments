#!/bin/bash


source base.sh

for ((i=1 ; i<=$1 ; i++ ));
do
  $QUERYMAIN -Dexec.args="$QUERYGLOBAL -output query-output-4-$i.log -result query-result-4-$i.log" 2>&1| tee $DIR/query-4-$i.log
done


