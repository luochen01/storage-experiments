#!/bin/bash

source ./base.sh

NCS=172.31.56.97,172.31.52.107,172.31.55.12,172.31.62.158

./create.sh

$MAIN -Dexec.args="$GLOBAL -scale 400 -u $NCS -workers 4" 2>&1| tee $DIR/load-master-4.log

./flush.sh  2>&1| tee -a $DIR/load-master-4.log

