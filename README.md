# ETLToolSuite-EntityGenerator

EntityGenerator is part of the **ETL Tool Suite** used for generating entity files that can be loaded into various datasource environments such as I2B2 and tranSMART.  
It provides command-line utilities for transforming source data and mapping files into standardized CSV entities suitable for downstream ingestion.
 
**Project Type:** Legacy Java 8 ETL Utility  
**Status:** Legacy migration in progress (Ant → Maven)

---

## Overview

EntityGenerator supports the generation of entity files used in biomedical ETL workflows.  
It can be run using either legacy Ant-based JARs or new **Maven-shaded executable JARs**.  
The modernization process introduces:
- Portable Maven build and deployment configuration
- Nexus integration (sandbox / AWS Dev / AWS Prod)
- Standardized Makefile targets for local and CI/CD pipelines

---

## Project Structure

| Path | Description |
|------|--------------|
| `src/` | Java source code (legacy structure) |
| `resources/` | Configuration files and dependencies |
| `dist/jars/` | Output directory for Ant-built JARs |
| `target/` | Output directory for Maven builds |
| `Makefile` | Wrapper for Maven build, test, and deploy commands |
| `pom.xml` | Maven project descriptor for multi-jar shaded builds |

---

## Build and Deployment Options

### Option 1: Legacy Ant Build (Supported)
Use this method while existing pipelines and ETL workflows are being migrated.

```bash
ant clean build
```

Outputs legacy JARs under `dist/jars/`.

---

### Option 2: Modern Maven Build (Preferred)

The Maven build creates reproducible, shaded (“fat”) JARs for each ETL tool.

```bash
# Build shaded JARs
mvn clean package

# Run a shaded jar (example)
java -jar target/entity-generator-1.0.0-SNAPSHOT-JsonMetadataGenerator.jar
```

Outputs all runnable artifacts under `target/`.

---

### Option 3: Makefile Workflow (Recommended for Local Developers and CI/CD)

The included `Makefile` standardizes common Maven goals and can serve as the foundation for both local and automated build pipelines.

```bash
make build      # Builds shaded JARs
make test       # Runs Maven tests
make deploy     # Deploys to local Nexus (port 8081)
make clean      # Cleans build directories
```

Using a Makefile as the top-level interface ensures consistent behavior between local development and CI/CD environments.  
It is generally considered good practice to **wrap CI/CD jobs around the Makefile**, especially in projects that require reproducible and portable builds across developer workstations and automation systems.

Credentials for Nexus can be configured in `~/.m2/settings.xml` or via environment variables:
```bash
export NEXUS_USERNAME=admin
export NEXUS_PASSWORD=admin123
```

---

## Deployment to Nexus (Sandbox)

The Maven `distributionManagement` block is preconfigured for local and AWS environments.

| Environment | Repository URL | Description                                                         |
|--------------|----------------|---------------------------------------------------------------------|
| Local Sandbox | `http://localhost:8081/repository/maven-releases/` | Developer testbed                                                   |
| AWS Dev (Future) | `https://nexus.dev.aws.example/...` | Dev environment (coming soon). AWS Service Instead of Nexus?        |
| AWS Prod (Future) | `https://nexus.prod.aws.example/...` | Prod Envirionment - AWS Service For AWS Hosted Env Instead of Nexus |

Deploy snapshots or releases:
```bash
mvn -DskipTests deploy
```

---

## Legacy Example – NHANES Dataset

This example remains unchanged for legacy users and documentation continuity.  
It demonstrates the original Ant-generated EntityGenerator workflow.  
Modernized equivalents will be introduced in future versions.

Steps below were validated on macOS and Amazon Linux.

1. Open a bash connection to your ETL Client Docker
   ```bash
   docker exec -e COLUMNS="`tput cols`" -e LINES="`tput lines`" -ti etl-client bash
   ```
2. Clone this project:
   ```bash
   git clone https://github.com/hms-dbmi/ETLToolSuite-EntityGenerator
   cd ETLToolSuite-EntityGenerator
   ```
3. Create working directories:
   ```bash
   mkdir data mappings completed
   ```
4. Copy data and mapping files generated from the [MappingGenerator Example](https://github.com/hms-dbmi/ETLToolSuite-MappingGenerator):
   ```bash
   cp ../ETLToolSuite-MappingGenerator/example/Asthma_Misior_GSE13168.txt data/
   cp ../ETLToolSuite-MappingGenerator/example/mapping.csv mappings/mapping.csv
   cp ../ETLToolSuite-MappingGenerator/example/mapping.csv.patient mappings/PatientMapping.csv
   ```
5. Run the EntityGenerator:
   ```bash
   java -jar EntityGenerator.jar -jobtype CSVToI2b2TM
   ```
6. Check output:
   ```bash
   cd completed
   ls -la
   ```
7. Expected files:
   ```
   I2B2.csv
   ConceptDimension.csv
   ObservationFact.csv
   ConceptCounts.csv
   TableAccess.csv
   PatientDimension.csv
   PatientTrial.csv
   PatientMapping.csv
   ```
8. Exit the container and proceed to data loading via  
   [WorkflowScripts → I2B2TM_V18_1](https://github.com/hms-dbmi/ETLToolSuite-WorkflowScripts/tree/master/oracle/ctl/I2B2TM_V18_1)

---

## Modernization Roadmap (Coming Soon)

| Feature | Status |
|----------|---------|
| Maven-based build and shading | Implemented |
| Nexus sandbox deployment | Supported |
| AWS Dev/Prod Nexus profiles | Coming soon |
| GitHub Actions / CI integration | Planned |
| Automated ETL pipeline packaging | Planned |
| Docker-based runtime container | Planned |

---

## License
Copyright © Harvard Medical School  
Licensed under the Apache License, Version 2.0.

---

## Acknowledgements
Developed and maintained by the Avillach Lab at Harvard Medical School as part of the **BioData Catalyst** initiative.  
Originally derived from the HMS ETL Tool Suite for i2b2/tranSMART integration.
