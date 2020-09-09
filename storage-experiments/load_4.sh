#!/bin/bash

source ./base.sh

NCS=172.31.62.37,172.31.52.186,172.31.54.225,172.31.52.138

./create.sh 4

$MAIN -Dexec.args="$GLOBAL -scale 400 -u $NCS -workers 4" 2>&1| tee $DIR/load-master-4.log

./finalize.sh 4
