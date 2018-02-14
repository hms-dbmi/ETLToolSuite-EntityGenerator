package etl.job.jobtype;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import utility.directoryutils.DirUtils;
import au.com.bytecode.opencsv.CSVReader;
import etl.data.datasource.CSVDataSource;
import etl.data.datasource.DataSource;
import etl.data.export.ETLFileWriter;
import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.Mapping;
import etl.mapping.PatientMapping;

public class DemoJob extends JobType {
	
	private static final String DATA_DIR = "/Users/tom/Documents/Cardia/"; 

	private static final String DESTINATION_DIR = "/Users/tom/Documents/Cardia/"; 
	
	private static final String WRITE_PROCESSING_DIR = "/Users/tom/Documents/Cardia/Processing/";
	
	private static final String WRITE_COMPLETED_DIR = "/Users/tom/Documents/Cardia/Completed/"; 

	private static final String DELIMITER = "\t"; 

	private static final String MAPPING_DIR = "/Users/tom/Documents/Cardia/Mapping/";
	//mappingcardia
	
	private static final String MAPPING_FILE = "/Users/tom/Documents/Cardia/mappingv2.txt";

	private static final String PATIENT_MAPPING_FILE = "/Users/tom/Documents/Cardia/PatientMapping.csv";
	
	private static final String MAPPING_DELIMITER = ",";
	
	private static final List<String> EXPORT_TABLES = new ArrayList<String>(Arrays.asList("PatientDimension", "ObservationFact", "ConceptDimension", "I2B2"));

	public Charset encoding = StandardCharsets.UTF_8;
	
	private static final String CLEAN_COMMENTS = "T";
	
	private final String sourceSystem = "CARDIA";
	
	private final boolean generateLoadScripts = true;
	
	private final String loadDialect = "Oracle";
	
	// these should be internal to the class leave as constant variable
	private static final String EXPORT_PACKAGE = "etl.data.export.i2b2.";
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	public static int totalFiles = 0;
	public DemoJob(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void runJob() {
		try {
			// clean processing dir
			
			try{
			
				DirUtils.removeAllFileFromDir(Paths.get(WRITE_PROCESSING_DIR));
			
			} catch (IOException e){
				
				System.err.println("Processing Dir could not be cleaned ");
				e.printStackTrace();
			}
			
			// Mapping prep
			DataSource ds = DataSource.initDataSourceType("CsvDataSource");
			
			CSVReader mappingReader = (CSVReader) ds.processData(MAPPING_FILE, MAPPING_DELIMITER);
			
			CSVReader patientMappingReader = (CSVReader) ds.processData(PATIENT_MAPPING_FILE, MAPPING_DELIMITER);
			
			Mapping mapping = Mapping.initMappingType("CsvToI2b2TMMapping");
			
			// each file name gets its own mapping object
			//Map<String, List<CsvToI2b2TMMapping>> mappings = CsvToI2b2TMMapping.class.newInstance().processMapping(mappingReader);			
			
			// same with patient mapper
			Map<String, List<PatientMapping>> patientMappings = PatientMapping.class.newInstance().processMapping(patientMappingReader);			

		} catch(Exception e){
			e.printStackTrace();
		}
	}

}
