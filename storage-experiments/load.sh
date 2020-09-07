#!/bin/bash

source ./base.sh

NCS=172.31.62.37,172.31.52.186

$MAIN -Dexec.args="$GLOBAL -scale 200 -u $NCS" 2>&1| tee $DIR/load-master-2.log


