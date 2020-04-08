#!/bin/bash

source ./base.sh
#wait 10s
dist=UNIFORM
u=0.1

for i in 5
do
	echo "dataset repair $i"
	out=$BASE/repair/repair-dataset-index-$i
	./create_validation_norepair_index.sh $i
	$REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_norepair_index_$i -rf $FREQ -rw $WAIT -rl $out.csv -repair Dataset" 2>&1 | tee "$out.log"
	./finalize_validation_norepair_index.sh $i drop

   echo "index repair $i"
   out=$BASE/repair/repair-index-index-$i
   ./create_validation_index.sh $i
   $REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_index_$i -rf $FREQ -rw $WAIT -rl $out.csv -repair Index -parallel 5" 2>&1 | tee "$out.log"
   ./finalize_validation_index.sh $i drop
done



