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
To see it in action follow here to [load the NHANES dataset](https://github.com/hms-dbmi/ETLToolSuite-EntityGenerator/edit/master/Example-NHANES.md)

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
