#!/bin/bash

source ./base.sh

dist=UNIFORM
u=0.1

for i in 1 2 3 4 5
do
    echo "deleted btree $i index"
   ./create_deletebtree_index.sh $i
    $MAIN -l $BASE/upsert/upsert-deletebtree-index-"$i".log -update $u -d $DURATION -dist $dist
   ./finalize_deletebtree_index.sh $i drop
    echo "sleeping for 1 hour..."    

     echo "validation norepair $i index"
   ./create_validation_norepair_index.sh $i
    $MAIN -l $BASE/upsert/upsert-validation-norepair-index-"$i".log -update $u -d $DURATION -dist $dist
   ./finalize_validation_norepair_index.sh $i drop

    echo "validation $i index"
   ./create_validation_index.sh $i
    $MAIN -l $BASE/upsert/upsert-validation-index-"$i".log -update $u -d $DURATION -dist $dist
   ./finalize_validation_index.sh $i drop

    echo "antimatter $i index"
   ./create_antimatter_index.sh $i
    $MAIN -l $BASE/upsert/upsert-antimatter-index-"$i".log -update $u -d $DURATION -dist $dist
   ./finalize_antimatter_index.sh $i drop
done




