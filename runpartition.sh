#!/bin/bash

for filename in ./resources/config.part*.config; do
    for ((i=0; i<=3; i++)); do
        java -jar EntityGenerator.jar -jobtype CSVToI2b2TM -propertiesfile $filename -Xmx12g
    done
done