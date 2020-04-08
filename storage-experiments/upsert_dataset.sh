#!/bin/bash

source ./base.sh
#80M
TOTAL=8000000
#TOTAL=10000
#i=5
#u=0.5 # no update
i=1
u=0
for dist in UNIFORM
do
   dv="$dist"_"$i"
   #echo "validation no repair $dist $u "
   #./create_validation_norepair.sh $dv
   #$MAIN -l $BASE/dataset/upsert-validation-norepair-"$dist"-"$u".log -update $u -t $TOTAL -dist $dist
   #./finalize_validation_norepair.sh $dv   

  #echo "validation $dist $u "
  #./create_validation.sh $dv
  #$MAIN -l $BASE/dataset/upsert-validation-"$dist"-"$u".log -update $u -t $TOTAL -dist $dist
  #./finalize_validation.sh $dv

 echo "antimatter $dist $u "
  ./create_antimatter.sh $dv
  $MAIN -l $BASE/dataset/upsert-antimatter-"$dist"-"$u".log -update $u -t $TOTAL -dist $dist
  ./finalize_antimatter.sh $dv

done




