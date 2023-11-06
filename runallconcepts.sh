#!/bin/bash

IFS=$'\r\n' GLOBIGNORE='*' command eval  'studyids=($(cat resources/studyids.txt))'

for studyid in ${studyids[@]}; do
   echo "running ${studyid}"
   if [ -d "data/${studyid}/" ]; then

     find data/${studyid}/ -type f -exec rm -rf {} \;

   fi
   aws s3 cp s3://stage-$studyid-etl/mappings/mapping.csv mappings/${studyid}/mapping.csv --quiet
   aws s3 cp s3://stage-$studyid-etl/data/ data/${studyid}/ --recursive --quiet
   aws s3 cp s3://stage-$studyid-etl/resources/job.config resources/${studyid}/job.config --quiet

   sed "s/pathseparator.*//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
   mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config
   
   if grep -q pathseparator "resources/${studyid}/job.config"; then
      sed "s/pathseparator.*/pathseparator=\\\\\\\\/g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config

      sed "s/filename.*/filename=data\/${studyid}\//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config

      sed "s/mappingfile.*/mappingfile=mappings\/${studyid}\/mapping.csv\//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config
      
      sed "s/patientmappingfile.*/patientmappingfile=mappings\/${studyid}\/mapping.csv.patient\//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config

   else
      echo "pathseparator=\\" >> resources/${studyid}/job.config

      sed "s/filename.*/filename=data\/${studyid}\//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config

      sed "s/mappingfile.*/mappingfile=mappings\/${studyid}\/mapping.csv\//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config
      
      sed "s/patientmappingfile.*/patientmappingfile=mappings\/${studyid}\/mapping.csv.patient\//g" ./resources/${studyid}/job.config > ./resources/${studyid}/job2.config
      mv ./resources/${studyid}/job2.config ./resources/${studyid}/job.config
   fi

   #nohup java -jar GenerateAllConcepts.jar -propertiesfile resources/job.config -Xmx5g &
   java -jar GenerateAllConcepts.jar -propertiesfile resources/${studyid}/job.config -Xmx5g

   #aws s3 cp completed/${studyid}_allConcepts.csv s3://stage-$studyid-etl/completed/${studyid}_allConcepts.csv

   #while [ $(ps aux | grep EntityGenerator.jar | wc -l) -gt 16 ]; do
   
   #   sleep 1
   
   echo "finished ${studyid}"
done