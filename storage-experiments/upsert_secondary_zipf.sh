#!/bin/bash

source ./base.sh

i=5
for dist in ZIPF
do
  for u in 0.5
  do
   dv="$dist"_"$i"
   echo "antimatter $dist $u "
   ./create_antimatter.sh $dv
   $MAIN -l $BASE/upsert/upsert-antimatter-"$dist"-"$u".log -update $u -d $DURATION -dist $dist
   ./finalize_antimatter.sh $dv drop

  echo "validation no repair $dist $u "
   ./create_validation_norepair.sh $dv
   $MAIN -l $BASE/upsert/upsert-validation-norepair-"$dist"-"$u".log -update $u -d $DURATION -dist $dist
   ./finalize_validation_norepair.sh $dv drop

   echo "validation $dist $u "
   ./create_validation.sh $dv
   $MAIN -l $BASE/upsert/upsert-validation-"$dist"-"$u".log -update $u -d $DURATION -dist $dist
   ./finalize_validation.sh $dv drop
   i=$(($i+1))   
  done
done