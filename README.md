# ETLToolSuite-EntityGenerator
This tool will generate Entity Files that can be used to load into Datasources.

**Author**: Thomas DeSain

***
**Example Prerequistes**
***
Admin rights to machine hosting docker    
[Quick Start docker stack](https://github.com/hms-dbmi/docker-images/tree/master/deployments/i2b2transmart/quickstart)     
[ETL Client Docker](https://github.com/hms-dbmi/etl-client-docker)    
Data file and Mapping file from [MappingGenerator Example](https://github.com/hms-dbmi/ETLToolSuite-MappingGenerator)    

***
Steps:  
This example was validated on a Mac and AMI Linux terminals.   

1. Open bash connection to your ETL Client Docker  
``docker exec -e COLUMNS="`tput cols`" -e LINES="`tput lines`" -ti etl-client bash``   
2. use git to clone this project to a dir of your choosing.
`git clone https://github.com/hms-dbmi/ETLToolSuite-EntityGenerator`     
3. Navigate to root directory:     
`cd ETLToolSuite-EntityGenerator`   
4. Make a directory to store your data:    
`mkdir data`   
5. Make a directory to store your mapping file:    
`mkdir mappings`   
6. Make a directory to store your processed data files:      
`mkdir completed`
7. Copy the data file and mapping file generated from the [MappingGenerator Example](https://github.com/hms-dbmi/ETLToolSuite-MappingGenerator)    
( The BASE_DIR will be the location of the MappingGenerator git project you cloned in the Mapping generator example ) :      
`cp ../ETLToolSuite-MappingGenerator/example/Asthma_Misior_GSE13168.txt data/`   
`cp ../ETLToolSuite-MappingGenerator/example/mapping.csv mappings/mapping.csv`    
`cp ../ETLToolSuite-MappingGenerator/example/mapping.csv.patient mappings/PatientMapping.csv`        
8. execute following code block to generate your I2B2 entities:    
`java -jar EntityGenerator.jar -jobtype CSVToI2b2TM`   
9. Navigate to completed directory
`cd completed`  
10. list the directory's contents.  
`ls -la`  
11. Once the job has completed processing this folder will contain the following files:    
	*I2B2.csv*  
	*ConceptDimension.csv*  
	*ObservationFact.csv*  
	*ConceptCounts.csv*
	*TableAccess.csv*   
	*PatientDimension.csv*   
	*PatientTrial.csv*    
	*PatientMapping.csv*   
12. exit
`exit`
13. If your data files exist you can now move on to loading the entity files into your database by following the [readme here](https://github.com/hms-dbmi/ETLToolSuite-WorkflowScripts/tree/master/oracle/ctl/I2B2TM_V18_1).
***

***
**Additional Data Loads Examples**   
The following will give examples on how to load subsets of Nhanes data into I2B2 / Transmart   
***   
**Example Prerequisites**   
***   
Admin rights to machine hosting docker.   
Quick Start docker stack   
ETL Client Docker   
   
Steps:   
1.	Open bash connection to your ETL Client Docker    
``docker exec -e COLUMNS="`tput cols`" -e LINES="`tput lines`" -ti etl-client bash``       
2.	Use git to clone this project to a dir of your choosing.    
`git clone https://github.com/hms-dbmi/ETLToolSuite-EntityGenerator`    
3.	Navigate to root directory of EntityGenerator:    
`cd ETLToolSuite-EntityGenerator`   
4.	Make a directory to store your processed data files:      
`mkdir completed`    
5.	There are two data sample sizes. Use one of the following commands to process data samples.       
**- Small Sample Size ( 99 Patients )**     
`java -jar EntityGenerator.jar -jobtype CSVToI2b2TM -propertiesfile resources/NhanesCSV.config`   
**- Large Sample Size ( 4997 Patients )**    
`java -Xmx6g -jar EntityGenerator.jar -jobtype CSVToI2b2TM -propertiesfile resources/NhanesCSV2.config`    
6.	Navigate to completed directory.   
`cd completed`    
7.	List the directory's contents   
`ls -la`   
8.	Once the job has completed processing this folder will contain the following files:   
I2B2.csv   
ConceptDimension.csv   
ObservationFact.csv   
ConceptCounts.csv   
TableAccess.csv   
PatientDimension.csv   
PatientTrial.csv   
PatientMapping.csv   
9.	exit the etl-client docker   
`exit`   
10.	Run following docker command on your machine hosting Quick Start Stack:   
( Take note of the IPv4Address before the CIDR notation ( / ) for the quickstart_db_1 container it will be needed in upcoming steps )   
`docker network inspect quickstart_public | grep -A4 quickstart_db_1`   
11.	Open bash connection to your ETL Client Docker   
``docker exec -e COLUMNS="`tput cols`" -e LINES="`tput lines`" -ti etl-client bash``   
12.	Use git to clone this project to a dir of your choosing. Change username to your git user.   
`git clone https://github.com/hms-dbmi/ETLToolSuite-WorkflowScripts`   
13.	Navigate to the following dir:   
`cd ETLToolSuite-WorkflowScripts`   
14.	Execute following command to load your Docker DB   
( docker host name of ip will be the IPv4Address found in step 10. )   
`bash ExampleLoad.sh <path_to_generated_entity_files> <docker_host_name_or_ip> <username_and_password_for_docker_db>`   
15.	You can verify all the data was loaded successfully by running following command and visiting your quickstart application.   
`cat *.log | grep -B1 -A4 'Rows successfully loaded'`   
