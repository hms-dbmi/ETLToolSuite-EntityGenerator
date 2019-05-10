#!/bin/bash

java -jar EntityGenerator.jar -jobtype CSVToI2b2TM -propertiesfile patient.config -Xmx12g

for filename in ./resources/config.part*.config; do

	nohup java -jar EntityGenerator.jar -jobtype CSVToI2b2TM -propertiesfile $filename -Xmx12g & >> logs/${filename}.log

        if [ $(ps aux --no-heading | grep EntityGenerator.jar | wc -l) -gt 10 ]
           then
		sleep 5
           	echo $(ps aux --no-heading | grep EntityGenerator.jar | wc -l)
	fi
done