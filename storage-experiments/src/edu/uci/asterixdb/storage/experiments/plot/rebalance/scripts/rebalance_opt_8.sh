#!/bin/bash


source ./base.sh

./rebalance_opt.sh 1,2,3,4,5,6,7 2>&1| tee $DIR/rebalance-opt-in-8.log

./flush.sh

./rebalance_opt.sh 1,2,3,4,5,6,7,8 2>&1| tee $DIR/rebalance-opt-out-8.log

./flush.sh


