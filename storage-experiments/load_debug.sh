#!/bin/bash

source ./base.sh

NCS=localhost

./create_debug.sh 1
$MAIN -Dexec.args="$GLOBAL -scale 1 -u $NCS" 2>&1| tee $DIR/load-master-2.log

#./finalize.sh 1

