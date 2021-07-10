#!/bin/bash

source ./base.sh

NCS=172.31.56.97,172.31.52.107

./create.sh

$MAIN -Dexec.args="$GLOBAL -scale 200 -u $NCS -workers 2" 2>&1| tee $DIR/load-master-2.log

./flush.sh  2>&1| tee -a $DIR/load-master-4.log

