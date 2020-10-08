#!/bin/bash

source ./base.sh

NCS=localhost

./create.sh
$MAIN -Dexec.args="$GLOBAL -scale 1 -u $NCS -workers 1 " 2>&1| tee $DIR/load-master-2.log


