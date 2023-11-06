#!/bin/bash

IFS=$'\r\n' GLOBIGNORE='*' command eval  'studyids=($(cat studyids.txt))'

for studyid in ${studyids[@]}; do
	rm -rf data/*
	rm -rf resources/job.config

	aws s3 cp s3://avillach-73-bdcatalyst-etl/${studyid}/data/ data/ --recursive
	aws s3 cp s3://avillach-73-bdcatalyst-etl/${studyid}/mappings/ mappings/ --recursive
	aws s3 cp s3://avillach-73-bdcatalyst-etl/${studyid}/resources/job.config resources/job.config

	java -jar GenerateAllConcepts.jar -propertiesfile resources/job.config -Xmx5g > resources/${studyid}/etl.log 2>&1 &

done