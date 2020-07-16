#!/bin/bash


curl -d "dataverseName=twitter&datasetName=ds_tweet&nodes=asterix_nc1&async_replicate=true" -X POST http://localhost:19002/admin/rebalanceopt

