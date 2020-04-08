#!/bin/bash

source ./base.sh
#80M
TOTAL=80000000
dist=UNIFORM
i=1
for u in 0 0.5
do
  
  dv="$dist"_"$i"
     echo "inplace $dist $u "
   ./create_inplace.sh $dv
   $MAIN -l $BASE/dataset/upsert-inplace-"$dist"-"$u".log -period $PERIOD -update $u -t $TOTAL -dist $dist
   ./finalize_inplace.sh $dv

  i=$((i+4))
done




