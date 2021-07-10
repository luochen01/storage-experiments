#!/bin/bash

source ./base.sh

NCS=172.31.63.144,172.31.51.23,172.31.57.217,172.31.48.229,172.31.56.97,172.31.52.107,172.31.55.12,172.31.62.158

./create.sh

$MAIN -Dexec.args="$GLOBAL -scale 800 -u $NCS -workers 8" 2>&1| tee $DIR/load-opt-8.log

./flush.sh  2>&1| tee -a $DIR/load-opt-8.log

