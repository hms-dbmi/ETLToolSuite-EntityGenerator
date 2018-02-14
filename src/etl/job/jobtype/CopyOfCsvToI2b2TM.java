package etl.job.jobtype;

import java.nio.file.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.carrotsearch.sizeof.BlackMagic;
import com.carrotsearch.sizeof.RamUsageEstimator;

import utility.ObjectSizeFetcher;
import utility.directoryutils.DirUtils;
import au.com.bytecode.opencsv.CSVReader;
import etl.data.datasource.CSVDataSource;
import etl.data.datasource.DataSource;
import etl.data.datasource.entities.CriteriaNode;
import etl.data.datasource.loader.Loader;
import etl.data.datasource.loader.loaders.Oracle;
import etl.data.datasource.loader.loaders.OracleControlFile;
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.PatientMapping;
import etl.data.export.ETLFileWriter;
import etl.data.export.ReportWriter;
import etl.data.export.entities.*;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;

public class CopyOfCsvToI2b2TM extends JobType {
	// All static constants should go into passed args or config file.
	//private static final String DATA_DIR = "/Users/tom/Documents/Cardia/"; 
	
	//private static final String WRITE_PROCESSING_DIR = "/Users/tom/Documents/Cardia/Processing/";
	
	//private static final String WRITE_COMPLETED_DIR = "/Users/tom/Documents/Cardia/Completed/"; 
	
	//private static final String MAPPING_DIR = "/Users/tom/Documents/Cardia/Mapping/";
	
//	private static final String MAPPING_FILE = "/Users/tom/Documents/Cardia/cardiamapping.v3.txt";
	
	//private static final String PATIENT_MAPPING_FILE = "/Users/tom/Documents/Cardia/PatientMapping.csv";

	private static final String DATA_DIR = "/Users/tom/Documents/JacksonMapping/load/"; 

	private static final String WRITE_PROCESSING_DIR = "/Users/tom/Documents/JacksonMapping/Processing/";
	
	private static final String WRITE_COMPLETED_DIR = "/Users/tom/Documents/JacksonMapping/Completed/"; 
	
	private static final String MAPPING_FILE = "/Users/tom/Documents/JacksonMapping/mapping.csv";
	
	private static final String PATIENT_MAPPING_FILE = "/Users/tom/Documents/JacksonMapping/PatientMapping.csv";

	private static final char DELIMITER = ','; 	

	private static final char MAPPING_DELIMITER = ',';
	
	private static final List<String> EXPORT_TABLES = new ArrayList<String>(Arrays.asList("PatientDimension", "ConceptDimension", "ObservationFact", "I2B2"));

	public Charset encoding = StandardCharsets.UTF_8;
	
	private static final String CLEAN_COMMENTS = "T";
	
	private final String sourceSystem = "CARDIA";
	
	private final boolean generateLoadScripts = true;
	
	private final String loadDialect = "Oracle";
	
	// these should be internal to the class leave as constant variable
	private static final String EXPORT_PACKAGE = "etl.data.export.i2b2.";
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	public static int totalFiles = 0;
	// constant initializers
	{
		Entity.ROOT_NODE = "Jackson Heart Study (JHS) Cohort";
		
		Entity.SOURCESYSTEM_CD = sourceSystem;;
	}
	
	public CopyOfCsvToI2b2TM(String str) throws Exception {
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
			CSVDataSource ds = new CSVDataSource("CSVFILE");
			
			CSVReader mappingReader = ds.processCSV(MAPPING_FILE, MAPPING_DELIMITER);
			
			CSVReader patientMappingReader = ds.processCSV(PATIENT_MAPPING_FILE, MAPPING_DELIMITER);
			
			// each file name gets its own mapping object
			Map<String, List<CsvToI2b2TMMapping>> mappings = CsvToI2b2TMMapping.class.newInstance().processMapping(mappingReader);			
			
			// same with patient mapper
			Map<String, List<PatientMapping>> patientMappings = PatientMapping.class.newInstance().processMapping(patientMappingReader);			
			
			/*
			 * iterate over keyset in mapping file
			*	Each key represents the filename for mapping entries
			*
			*/
			Set<String> keys = mappings.keySet();
			
			// Map used to store Entity
			// Will be EntityName and HashSet of Entity objects
			// These entities can then be manipulated and written to files for loading.
			//Map<String,Set<Entity>> dumpMap = new HashMap<String,Set<Entity>>();
			
			Map<String,Set<Entity>> entityMap = new HashMap<String,Set<Entity>>();
			
			//List<Entity> entityMaster = new ArrayList<Entity>();
			Set<Entity> patEntities = new HashSet<Entity>();

			ETLFileWriter etlFileWriter = new ETLFileWriter();
			List<String> errors = new ArrayList<String>();

			//keys.forEach(key -> {
			for(String key: keys){
				
				System.err.println("Files Processed: " + totalFiles);			
				this.totalFiles++;
				
				
				try {
					
					CSVReader dataReader = ds.processCSV(DATA_DIR + key, DELIMITER);
					
					List<String[]> dataList = dataReader.readAll();
					
					List<Map<String, String>> dataMap = buildDataMap(dataReader);
					
					String header = null;
					// remove comments
					// Clean List
					List<String[]> badRows = new ArrayList<String[]>();
					
					if(CLEAN_COMMENTS.equalsIgnoreCase("T")){
						
						for(String[] strings:dataList){
							
							if(strings[0].isEmpty() || strings[0].substring(0, 1).matches("[^A-Za-z0-9]")){
								
								badRows.add(strings);
								
							} else {
								
								break;
								
							}
								
						}
						
					}

					dataList.removeAll(badRows);
					
					String hasHeader = "T";
					
					if(hasHeader.equalsIgnoreCase("T")){
						
						dataList.remove(0);
						
					}
					
					// Need to rebuild this a bit
					// could be a bit cleaner. should only need one build entity in Entity super class
					// Need to make an abstract Mapping layer that all mappings extend haven't decided best way to handle this.

					Set<Entity> entities = Entity.buildEntities(EXPORT_TABLES, dataList, header, mappings.get(key));
					
					//entityMaster.addAll(new ArrayList<Entity>(entities));
					
					/*
					//entities.stream().forEach(entity -> {
					for(Entity entity: entities){
						
						String entKey = entity.getClass().getSimpleName();
						
						if(entityMap.containsKey(entKey)){
														
							Set<Entity> currentEntities = entityMap.get(entKey);
							
							currentEntities.add(entity);
							
							entityMap.put(entKey, currentEntities);

						} else {
							
							Set<Entity> currentEntities = new HashSet<Entity>();

							currentEntities.add(entity);
							
							entityMap.put(entKey, currentEntities);
							
						};
						
					}*/
					//);
					//System.out.println(com.carrotsearch.sizeof.RamUsageEstimator.sizeOf(entityMap));
					//ReportWriter.WRITE_COMPLETED_DIR = this.WRITE_COMPLETED_DIR;
					
					//ReportWriter.WRITE_PROCESSING_DIR = this.WRITE_PROCESSING_DIR;
					
					etlFileWriter.WRITE_COMPLETED_DIR = this.WRITE_COMPLETED_DIR;
					
					etlFileWriter.WRITE_PROCESSING_DIR = this.WRITE_PROCESSING_DIR;
					
					etlFileWriter.writeToFile(entities, true);
					
				} catch (Exception e) {
					
					System.err.println("Error Processing Datafile: " + key);
					
					errors.add("Error Processing Datafile: " + key + "\n");
					
					e.printStackTrace();

					errors.add(e.getMessage());
					
				} finally {
					//private static final String WRITE_PROCESSING_DIR =å "/Users/tom/Documents/Cardia/Processing/";
					etlFileWriter.getWriterMap().forEach((k,v) ->{
						try {

							v.flush();
							//v.close();

						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					});
					
					etlFileWriter.writeToFile(errors, WRITE_COMPLETED_DIR + "bad.log");
					
				}
				
				//private static final String WRITE_COMPLETED_PROCESSING_DIR = "/Users/tom/Documents/Cardia/Completed/"; 
				// Move Processed into completed folder
			}
			etlFileWriter.getWriterMap().forEach((k,v) ->{
				try {

					//v.flush();
					v.close();

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

			//);
			//System.out.println(dumpMap.size());

			patEntities.addAll(Entity.buildPatientEntities(EXPORT_TABLES, patientMappings));
			
			ETLFileWriter.class.newInstance().writeToFile(patEntities,true);

			Loader loader = Loader.initLoaderType("Oracle");
			
			Map<String, OracleControlFile> ocfs = loader.generateLoadTest(EXPORT_TABLES);
			
			ocfs.forEach((k,v) ->{
				
				try {
					
					ETLFileWriter.class.newInstance().writeToFile(v.toString(), WRITE_PROCESSING_DIR + k + ".ctl");
					
				} catch (Exception e) {
					System.err.println("Error writing control files");
					e.printStackTrace();
				}
				//System.out.println(v.toString());
				
			});
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// move processing files to completed
		try(DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(WRITE_PROCESSING_DIR))){
			
			for(Path file: stream){
				
				System.err.println(file.getFileName());
				
				Files.move(file, Paths.get(WRITE_COMPLETED_DIR + file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
				
			}
			
		} catch(IOException | DirectoryIteratorException e) {
			e.printStackTrace();
			
		}	
	}

	private List<Map<String, String>> buildDataMap(CSVReader dataReader) {
		
		List<String[]> recs;
		
		List<Map<String,String>> lmap = new ArrayList<Map<String,String>>();

		try {
			recs = dataReader.readAll();

			for(String[] rec:recs){
				
				Integer x = 0;
				
				for(String col: rec){
					
					Map<String,String> map = new HashMap<String,String>();
					
					map.put(x.toString(), col);
					x++;
				}
				
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return lmap;
		
	}
	
}
