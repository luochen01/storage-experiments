#!/bin/bash

source ./base.sh
#wait 10s
i=1
dist=UNIFORM
  for u in 0 0.5
  do
   dv="index"_"$i"
   echo "index repair $dist $u"
   out=$BASE/repair/repair-index-$dist-$u
   ./create_validation.sh $dv
   $REPAIR -Dexec.args="-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Index" 2>&1 | tee "$out.log"
   ./finalize_validation.sh $dv

  dv="index"_nobf_"$i"
   echo "index repair $dist $u"
   out=$BASE/repair/repair-index-$dist-$u-nobf
   ./create_validation_nocorrelate.sh $dv
   $REPAIR -Dexec.args="-u $-l $out.out -period $PERIOD -m RANDOM -update $u -t $TOTAL -dist $dist -dv twitter_validation_$dv -rf $FREQ -rw $WAIT -rl $out.csv -repair Index" 2>&1 | tee "$out.log"
   ./finalize_validation_nocorrelate.sh $dv
  i=$((i+4))
  done

