#!/bin/bash


source base.sh

for ((i=1 ; i<=$1 ; i++ ));
do
  $QUERYMAIN -Dexec.args="$QUERYGLOBAL -output query-profile-output-4-$i.log -result query-profile-result-4-$i.log -profile -query 18" 2>&1| tee $DIR/query-profile-4-$i.log
done


