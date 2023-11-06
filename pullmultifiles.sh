#!/bin/bash

IFS=$'\r\n' GLOBIGNORE='*' command eval  'studyids=($(cat studyids.txt))'

rm -rf data/*

for studyid in ${studyids[@]}; do
        aws s3 cp s3://avillach-73-bdcatalyst-etl/${studyid}/rawData/data/ data/ --recursive --exclude "*" --include "*Subject.multi*" --include "*Subject.Multi*" --include "*Subject.MULTI*"

done