#!/bin/bash


curl -d "dataverseName=tpch&nodes=1" -X POST http://localhost:19002/admin/rebalance

