#!/bin/bash


source ./base.sh

./rebalance.sh 1,2,3,4,5,6,7 2>&1| tee $DIR/rebalance-in-8.log

./flush.sh

./rebalance.sh 1,2,3,4,5,6,7,8 2>&1| tee $DIR/rebalance-out-8.log

./flush.sh


