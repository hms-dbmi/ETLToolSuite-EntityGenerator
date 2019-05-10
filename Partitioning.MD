# ETL Workflow  
Author: Tom DeSain

**Overview:**  
This document details the curation processes that a csv file would go through in order to be loaded into an I2B2 / Transmart instance.

**Data Storage:**  
All data is to be stored in a secure s3 bucket that should only be accessible to IRB approved personell and services. 

Config and Mapping files

A projects s3 bucket should be structered as so:
Base bucket:
<PROJECT_NAME>_ETL/

**Internal buckets:**  
<PROJECT_NAME>_ETL/completed - this will be used to optionally store processed data after the ETL process has been completed
<PROJECT_NAME>_ETL/data - All data that is required to be processed needs to be stored here
<PROJECT_NAME>_ETL/mappings - Any mapping files should be stored here, including the patient and data mapping files.
<PROJECT_NAME>_ETL/resources - Any configuration files should be stored here.

**Step one:**  
Data Curator process
The Data curator is meant to be run on the raw data files to purge any invalid application characters.    
These characters include all non-ascii characters as well as the following characters and there unicode equivalent:  
```
	* - \u002A
	| - \u007C
	/ - \u002F
	< - \u003C
	\ - \u005C
	" - \u0022
	' = \u0027
	: - \u003A
	? - \u003F
```  
After the data curator has completed the files will need to be synced to the aws s3 bucket ( <PROJECT_NAME>_ETL/data ).  
  
**Step two:**   
Data Evaluation process ( Currently Beta )  
The data evaluator is meant to give a predetermined estimate on the expected generated entities ( Concepts and Facts ).  
This is not only extremelly useful data validations but is essential in determining the size of the data load.  
  
This process will generate the following reports in the resources folder:  
* conceptevaluation.txt - This file shows the mappingkey, the evaluation type, and the number of expected concepts to be generated per mapping.  
	> Important to note that all Numeric concepts should have an expected generation of 1 concept due to the design of the application.  
* factevaluation.txt - This file shows the mappingkey, the evaluation type, and the number of expected facts to be generated per mapping.  
* dataevaluation.txt - A summary of totals for each entity.  
  
**Step three:**  
Partitioner  
The evaluation files must be stored in the resources folder in order to partition the data as it uses this to control sequences.  
  
This is designed to take a single mapping file and split it into a single job per row in the mapping file.  
   
After the partitioner completes its process the following files should exist:  
In the mappings folder each mapping will be named using this pattern:   
mapping.part<PARTITION_NUMBER>.mapping  
  
In the resources folder there will be a configuration generated for each mapping named in this pattern:  
config.part<PARTITION_NUMBER>.config  
   
**Step four:**  
Entity Generator  
After the Partitioner runs you can execute all the jobs by simply running the runpartitions.sh script located in the root folder.  
  
Once the process is complete the entity generator will create all the necessary data for a I2B2/Transmart's instance.  
  
All entities files are stored in the completed folder.  
  
**Step Five: ( Oracle instance only )**  
In order to load the data into a I2B2/Transmart instance simply run the DataLoad.sh script with necessary arguments to authenticate with an oracle instance.  

