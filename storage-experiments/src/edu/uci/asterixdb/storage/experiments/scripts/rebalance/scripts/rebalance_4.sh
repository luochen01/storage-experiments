#!/bin/bash


source ./base.sh

./rebalance.sh 1,2,3 2>&1| tee $DIR/rebalance-in-4.log

./flush.sh

#./rebalance.sh 1,2,3,4 2>&1| tee $DIR/rebalance-out-4.log



