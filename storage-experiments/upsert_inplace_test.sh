#!/bin/bash

source ./base.sh

for dist in UNIFORM
do
  i=1
  for u in 0.5
  do
   dv="$dist"_"$i"
   echo "inplace $dist $u "
   ./create_inplace.sh $dv
   $MAIN -l $BASE/upsert/upsert-inplace-"$dist"-"$u".log -update $u -d $DURATION -dist $dist
   ./finalize_inplace.sh $dv drop
   i=$(($i+5)) 
 done
done

