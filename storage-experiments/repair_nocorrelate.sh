#!/bin/bash

source ./base.sh
#wait 10s
dist=UNIFORM
u=0.1
record=1000
  i=1
  dv="index"_bf_"$i"_$record
  echo "index repair $dist $i $record $merge"
  out=$BASE/repair/repair-index-nobf-$dist-$u-$record
  ./common/create_validation_nocorrelate.sh $dv
  $REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Index -size $record" 2>&1 | tee "$out.log"
  ./common/finalize_validation_nocorrelate.sh $dv drop
done

dist=UNIFORM
u=0.1

i=5
echo "index repair $i"
out=$BASE/repair/repair-index-index-nobf-$i
./common/create_validation_index_nocorrelate.sh $i
$REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_index_$i -rf $FREQ -rw $WAIT -rl $out.csv -repair Index" 2>&1 | tee "$out.log"
./common/finalize_validation_index_nocorrelate.sh $i drop




