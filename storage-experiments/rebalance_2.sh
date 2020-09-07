#!/bin/bash


./rebalance_in.sh 1 2>&1| tee $DIR/rebalance-in-2.log

./rebalance_out.sh 1,2 2>&1| tee $DIR/rebalance-out-2.log



