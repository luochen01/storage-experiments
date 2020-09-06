#!/bin/bash


curl -d "dataverseName=tpch&nodes=2" -X POST http://localhost:19002/admin/rebalance

