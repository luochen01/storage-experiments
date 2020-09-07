#!/bin/bash


source ./base.sh

./rebalance.sh 1 2>&1| tee $DIR/rebalance-in-2.log

sleep 10m

./rebalance.sh 1,2 2>&1| tee $DIR/rebalance-out-2.log



