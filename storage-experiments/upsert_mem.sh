#!/bin/bash

source ./base.sh

./create_mem.sh
$MAIN -l $BASE/upsert_mem_append.log -update 0 -d 21600 -dist UNIFORM -t 100000000 
./finalize_mem.sh





