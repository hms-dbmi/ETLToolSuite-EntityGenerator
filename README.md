# ETLToolSuite-EntityGenerator
This tool will generate Entity Files that can be used to load into Datasources.

**Author**: Thomas DeSain

***
**Example Prerequistes**
***
Java8  
git  
admin rights  
[Quick Start docker stack](https://github.com/hms-dbmi/docker-images/tree/master/deployments/i2b2transmart/quickstart)     
[ETL Client Docker](https://github.com/hms-dbmi/etl-client-docker)    
Data file and Mapping file from [MappingGenerator Example](https://github.com/hms-dbmi/ETLToolSuite-MappingGenerator)    

***
Steps:  
This example was validated on a Mac and AMI Linux terminals   

1. Open bash connection to your ETL Client Docker  
`docker exec -it etl-client bash`   
2. use git to clone this project to a dir of your choosing. Change username to your git user.  
`git clone https://username@github.com/hms-dbmi/ETLToolSuite-EntityGenerator`     
3. cd to root directory:     
`cd ETLToolSuite-EntityGenerator`   
4. Make a directory to store your data:    
`mkdir data`   
5. Make a directory to store your mapping file:    
`mkdir mappings`   
6. Make a directory to store your processed data files:      
`mkdir completed`  
7. Copy the data file and mapping file generated from the [MappingGenerator Example](https://github.com/hms-dbmi/ETLToolSuite-MappingGenerator)    
( The BASE_DIR will be the location of the MappingGenerator git project you cloned in the Mapping generator example ) :      
`cp <BASE_DIR>/example/Asthma_Misior_GSE13168.txt data`   
`cp <BASE_DIR>/example/GSE13168_Mapping.txt mappings\mapping.csv`    
`cp <BASE_DIR>/example/GSE13168_Mapping.csv.patient mappings\PatientMapping.csv`     
8. execute following code block to generate your I2B2 entities:    
`java -jar EntityGenerator.jar -jobtype CSVToI2b2TM2New2`   
9. cd completed directory
`cd completed`  
10. list the directory's contents.  
`ls -la`  
11. Once the job has completed processing this folder will contain the following files:
I2B2.csv  
ConceptDimension.csv   
ObservationFact.csv   
ConceptCounts.csv   
TableAccess.csv   
PatientDimension.csv   
PatientTrial.csv   

12. If your data files exist you can now move on to loading the entity files into your database by following the [readme here](https://github.com/hms-dbmi/ETLToolSuite-WorkflowScripts/tree/master/oracle/ctl/I2B2TM_V18_1).
***

