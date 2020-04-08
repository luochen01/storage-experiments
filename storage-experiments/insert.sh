#!/bin/bash

source ./base.sh

DURATION=43200
i=0

for u in 0 0.5
do
   ./create_insert.sh $i
   $MAIN -l $BASE/insert/insert-"$u".log -update $u -d $DURATION
   ./finalize_insert.sh $i drop
   
   #nopk
   ./create_insert_nopk.sh $i
   $MAIN -l $BASE/insert/insert-nopk-"$u".log -update $u -d $DURATION
   ./finalize_insert_nopk.sh $i drop
   i=$(($i+5))   
done


