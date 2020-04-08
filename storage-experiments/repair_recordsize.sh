#!/bin/bash

source ./base.sh
dist=UNIFORM
u=0.1

record=1000
i=1
dv="index"_"$i"_$record
echo "index repair $dist $i $record $merge"
out=$BASE/repair/repair-index-$dist-$u-$record
./create_validation.sh $dv
$REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Index -size $record" 2>&1 | tee "$out.log"
./finalize_validation.sh $dv drop

dv="dataset"_"$i"_$record
echo "dataset repair $dist $i $record $merge"
out=$BASE/repair/repair-dataset-$dist-$u-$record
./create_validation_norepair.sh $dv
$REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_norepair_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Dataset -size $record" 2>&1 | tee "$out.log"
./finalize_validation_norepair.sh $dv drop




