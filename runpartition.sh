#!/bin/bash

while getopts m:j:c:r: option
do
case "${option}"
in
        m) memory=${OPTARG};;
        j) maxjobs=${OPTARG};;
        c) configfile=${OPTARG};;
        r) resdir=${OPTARG};;
esac
done

for filename in ${resdir}${configfile}; do
        nohup java -jar GenerateAllConcepts.jar -propertiesfile $filename -Xmx${memory} & 

        if [ $(ps aux | grep GenerateAllConcepts.jar | wc -l) -gt ${maxjobs} ]
           then
                sleep 5
        fi
done