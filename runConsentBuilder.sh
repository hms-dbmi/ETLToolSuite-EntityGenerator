#!/bin/bash

IFS=$'\r\n' GLOBIGNORE='*' command eval  'studyids=($(cat resources/studyids.txt))'

conceptcdseq=1200000
find completed/ -type f -exec rm -rf {} \;
for studyid in ${studyids[@]}; do
   find data/ -type f -exec rm -rf {} \;
   find data/ -type d -exec rm -rf {}/* \;
   #find completed/ -type f -exec rm -rf {} \;
   find processing/ -type f -exec rm -rf {} \;

   aws s3 cp s3://stage-${studyid}-etl/mappings/mapping.csv.patient mappings/mapping.csv.patient
   
   aws s3 cp s3://stage-${studyid}-etl/mappings/consentmapping.csv mappings/mapping.csv

   #aws s3 cp s3://stage-${studyid}-etl/resources/job.config resources/job.config
   
   aws s3 cp s3://stage-${studyid}-etl/data/ConsentGroupVariable.csv data/ConsentGroupVariable.csv
   
   aws s3 cp s3://stage-${studyid}-etl/completed/PatientMapping.csv completed/PatientMapping.csv
   
   aws s3 cp s3://stage-${studyid}-etl/completed/PatientTrial.csv completed/PatientTrial.csv

   aws s3 cp s3://stage-${studyid}-etl/completed/PatientDimension.csv completed/PatientDimension.csv

   lines=$(wc -l < data/ConsentGroupVariable.csv | tr -d ' ' )
   echo $lines
   conceptcdseq=$(($lines + $conceptcdseq))
   echo $conceptcdseq
   sed "s/sequencepatient=Y/sequencepatient=N/" ./resources/job.config > ./resources/job2.config
   sed "s/trialid=.*/trialid=CONSENTS/" ./resources/job2.config > ./resources/job3.config

   sed "s/conceptcdstartseq=.*/conceptcdstartseq=${conceptcdseq}/" ./resources/job3.config > ./resources/job.config

   java -jar EntityGenerator.jar -propertiesfile resources/job.config
   
   java -jar DataMerge.jar -propertiesfile resources/job.config
   #mv ./resources/job2.config ./resources/job.config

   #java -jar EntityGenerator.jar -propertiesfile resources/job.config
   
   #java -jar DataMerge.jar -propertiesfile resources/job.config
   
   #java -jar FillInTree.jar -propertiesfile resources/job.config
   
   #java -jar FixPaths.jar -propertiesfile resources/job.config
   
   #aws s3 cp completed/ s3://stage-general-etl/consententities/$studyid/ --recursive
   
done

java -jar FillInTree.jar -propertiesfile resources/job.config
java -jar FixPaths.jar -propertiesfile resources/job.config

rm -rf completed/Patient*
mv completed/ConceptDimensionnew.csv completed/ConceptDimension.csv
mv completed/I2B2new.csv completed/I2B2.csv
aws s3 cp completed/ s3://stage-general-etl/consententities/ --recursive

