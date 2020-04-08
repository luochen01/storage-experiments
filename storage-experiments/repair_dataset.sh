#!/bin/bash

source ./base.sh
i=1
dist=UNIFORM
  for u in 0 0.5
  do
   dv="dataset"_"$i"
   echo "dataset repair $dist $u"
   out=$BASE/repair/repair-dataset-$dist-$u
   ./create_validation_norepair.sh $dv
   $REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_norepair_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Dataset" 2>&1 | tee "$out.log"
   ./finalize_validation_norepair.sh $dv drop
  #with compaction
    dv="dataset_compact"_"$i"
    echo "dataset repair compact $dist $u"
   ./create_validation_norepair.sh $dv
   out=$BASE/repair/repair-dataset-compact-$dist-$u
   $REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_norepair_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Dataset -compact" 2>&1 | tee "$out.log"
   ./finalize_validation_norepair.sh $dv drop
  i=$((i+4))

  done




