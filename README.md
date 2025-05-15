# ETLToolSuite-EntityGenerator
This tool will generate Entity Files that can be used to load into Datasources.

**Author**: Thomas DeSain

***
## Building the Project
***

### Running the Ant Build to Generate JAR Files

To build the project and generate new JAR files, you need to run the Ant build system with the default "build" target:

1. Navigate to the project root directory

2. Run the Ant build command:
   ```
   ant
   ```

   This will execute the default "build" target defined in the build.xml file.

3. If you want to ensure a clean build (recommended when making changes), you can run:
   ```
   ant clean build
   ```

   This will first clean the bin directory and then perform a full build.

#### What Happens During the Build

The build process will:
1. Clean the bin directory (if the clean target is called)
2. Create the bin directory (init target)
3. Compile all Java source files
4. Package the compiled classes into multiple JAR files

The generated JAR files will be placed in the `../ETL-MissionControl-dbgap-submodule/jars/` directory, relative to the project root.

#### Notes

- The build.xml file defines multiple JAR files that will be created during the build process.
- Some JAR definitions in the build.xml are commented out (lines 125-361), so those JARs won't be generated unless you uncomment those sections.
- The build uses Java 1.8 as the target and source compatibility level.

### Installing Apache Ant

If you don't have Ant installed, you'll need to install it before you can build the project:

#### macOS

**Using Homebrew (Recommended)**
```bash
brew install ant
```

**Using MacPorts**
```bash
sudo port install apache-ant
```

**Manual Installation**
1. Download the latest Ant binary from [Apache Ant Downloads](https://ant.apache.org/bindownload.cgi)
2. Extract it to a directory of your choice (e.g., `/usr/local/ant`)
3. Add Ant to your PATH in your `.bash_profile` or `.zshrc`:
   ```bash
   export ANT_HOME=/usr/local/ant
   export PATH=$PATH:$ANT_HOME/bin
   ```
4. Reload your profile: `source ~/.bash_profile` or `source ~/.zshrc`

#### Windows

**Using Chocolatey**
```
choco install ant
```

**Manual Installation**
1. Download the latest Ant binary from [Apache Ant Downloads](https://ant.apache.org/bindownload.cgi)
2. Extract it to a directory (e.g., `C:\apache-ant`)
3. Add the bin directory to your PATH:
   - Right-click on "This PC" or "My Computer" → Properties → Advanced system settings → Environment Variables
   - Add a new system variable ANT_HOME with the value of your Ant installation directory
   - Edit the PATH variable and add `%ANT_HOME%\bin`

#### Linux

**Using Package Manager**
For Debian/Ubuntu:
```bash
sudo apt-get update
sudo apt-get install ant
```

For Red Hat/Fedora/CentOS:
```bash
sudo yum install ant
```

**Manual Installation**
1. Download the latest Ant binary from [Apache Ant Downloads](https://ant.apache.org/bindownload.cgi)
2. Extract it: `tar -xzf apache-ant-*.tar.gz`
3. Move it to a suitable location: `sudo mv apache-ant-*/ /opt/ant`
4. Add to your PATH in `.bashrc`:
   ```bash
   export ANT_HOME=/opt/ant
   export PATH=$PATH:$ANT_HOME/bin
   ```
5. Reload your profile: `source ~/.bashrc`

#### Verifying Installation

After installation, verify Ant is properly installed:
```bash
ant -version
```

You should see output like: `Apache Ant(TM) version 1.10.x compiled on [date]`

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
To see it in action follow here to [load the NHANES dataset](https://github.com/hms-dbmi/ETLToolSuite-EntityGenerator/blob/master/Example-NHANES.md)

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
