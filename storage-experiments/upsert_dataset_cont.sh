#!/bin/bash

source ./base.sh
#80M
#TOTAL=80000000
TOTAL=100000
 	 $MAIN -l /tmp/twitter.log -update 0 -t $TOTAL -dist UNIFORM -limit 10000 -s $TOTAL
#  	./finalize.sh $dv



