#!/bin/bash


curl -d "dataverseName=Metadata&datasetName=CompactionPolicy" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Dataset" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=DatasourceAdapter" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Datatype" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Dataverse" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=ExternalFile" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Feed" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=ExternalFile" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=FeedConnection" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Function" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Index" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Library" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Node" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Nodegroup" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=Metadata&datasetName=Synonym" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Region" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Nation" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Customer" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=LineItem" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Orders" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Part" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Partsupp" -X POST http://localhost:19002/connector
printf "\n"
curl -d "dataverseName=tpch&datasetName=Supplier" -X POST http://localhost:19002/connector
printf "\n"

