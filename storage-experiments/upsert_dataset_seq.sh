#!/bin/bash

source ./base.sh
#80M
TOTAL=80000000
#TOTAL=10000
#i=5
#u=0.5 # no update
i=1
u=0
for dist in UNIFORM
do
   dv="$dist"_"$i"
   echo "validation no repair $dist $u "
   ./create_validation_norepair_seq.sh $dv
   $MAIN -l $BASE/dataset/upsert-validation-norepair-"$dist"-"$u".log -update $u -t $TOTAL -dist $dist -m Sequential
   ./finalize_validation_norepair_seq.sh $dv   

done




