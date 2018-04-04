package etl.job.jobtype;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
import etl.data.datasource.CSVDataSource;
import etl.data.datatype.DataType;
import etl.data.export.ETLFileWriter;
import etl.data.export.Export;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.PatientDimension;
import etl.job.jobtype.properties.JobProperties;
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.PatientMapping;
import utility.directoryutils.DirUtils;

public class CsvToI2B2TM extends JobType {
	// Parameters to go in a config file
	private static String DATA_DIR; 

	private static String WRITE_PROCESSING_DIR;
	
	private static String WRITE_COMPLETED_DIR; 
	
	private static String MAPPING_FILE;
	
	private static String PATIENT_MAPPING_FILE;
	
	private static OpenOption[] WRITE_OPTIONS;
	
	private static String SKIP_MAPPER_HEADER;
	
	private static String SKIP_DATA_HEADER;
	
	private static String ROOT_NODE;
	
	private static List<String> EXPORT_TABLES;

	// Void Parameters
	
	private static char DELIMITER; 	

	private static char MAPPING_DELIMITER;

	// class variables will remain
	public static int totalFiles = 0;
	
	private final static int skipMappingHeader = SKIP_DATA_HEADER == "Y" ? 1 : 0;;
	
	private final static int skipDataHeader = SKIP_MAPPER_HEADER == "Y" ? 1 : 0; ;
	
	private static final String EXPORT_PACKAGE = "etl.data.export.i2b2.";
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	public CsvToI2B2TM(String str) throws Exception {
		super(str);
	}

	
	@Override
	public void setVariables(JobProperties jobProperties) {
		// Required variables
		
		DATA_DIR = jobProperties.getProperty("datadir"); 
		
		MAPPING_FILE = jobProperties.getProperty("mappingfile");
		
		PATIENT_MAPPING_FILE = jobProperties.getProperty("patientmappingfile");
		
		// Variables with a default

		WRITE_PROCESSING_DIR = jobProperties.containsKey("processingdir") ? jobProperties.getProperty("processingdir") : "./Processing/";
		
		WRITE_COMPLETED_DIR = jobProperties.containsKey("completeddir") ? jobProperties.getProperty("completeddir") : "./Completed/";
		
		WRITE_OPTIONS = jobProperties.containsKey("writeoptions") ? buildOpenOption( jobProperties.getProperty("writeoptions") ) : new OpenOption[] { WRITE, CREATE, APPEND };
		
		SKIP_MAPPER_HEADER = jobProperties.containsKey("skipmapperheader") ? jobProperties.getProperty("skipmapperheader") : "Y";
		
		SKIP_DATA_HEADER = jobProperties.containsKey("skipdataheader") ? jobProperties.getProperty("skipdataheader") : "Y";
		
		ROOT_NODE = jobProperties.containsKey("rootnode") ? jobProperties.getProperty("rootnode") : "";
		
		// Void Parameters
		
		DELIMITER = jobProperties.containsKey("datadelimiter") && jobProperties.getProperty("datadelimiter").toCharArray().length == 1 ? 
				jobProperties.getProperty("datadelimiter").toCharArray()[0] : ','; 	

		MAPPING_DELIMITER = jobProperties.containsKey("mappingdelimiter") && jobProperties.getProperty("mappingdelimiter").toCharArray().length == 1 ? 
				jobProperties.getProperty("mappingdelimiter").toCharArray()[0] : ',';;
		
		// previously static initialization block
		
		Entity.ROOT_NODE = ROOT_NODE;
		
		DataType.ROOT_NODE = ROOT_NODE;
		
		DataType.DEFAULT_SOURCESYSTEM_CD = jobProperties.containsKey("sourcesystemcd") ? jobProperties.getProperty("sourcesystemcd") : "NOT GIVEN";
		
		EXPORT_TABLES = jobProperties.containsKey("exporttables") ? 
				new ArrayList<String>(Arrays.asList(jobProperties.getProperty("exporttables") )) 
				: new ArrayList<String>(Arrays.asList("PatientDimension", "ConceptDimension", "ObservationFact", "I2B2"));
		
	}
	
	private OpenOption[] buildOpenOption(String options) {
		
		OpenOption[] oarr = new OpenOption[options.split(",").length - 1];
		
		int x = 0;
		
		for(String option: options.split(",")) {
			
			if(option.equalsIgnoreCase("write")) oarr[x] = WRITE;
			
			if(option.equalsIgnoreCase("CREATE")) oarr[x] = CREATE;
			
			if(option.equalsIgnoreCase("APPEND")) oarr[x] = APPEND;
			
		}
		
		return oarr;
	}
			
			
	@Override
	public void runJob(JobProperties jobProperties) {
		
		setVariables(jobProperties);
		
		// used to store entities that will be processed through this job
		Set<Entity> entities = new HashSet<Entity>();
	
		// clean processing directory
		try{
		
			DirUtils.removeAllFileFromDir(Paths.get(WRITE_PROCESSING_DIR));
		
		} catch (IOException e){
			
			//System.err.println("Processing Dir could not be cleaned ");
			e.printStackTrace();
		}
		
			
		try {
			
			// need to switch this to initializer in super and use datasource type as Class name.
			CSVDataSource ds = new CSVDataSource("CSVFILE");

			CSVReader mappingReader = ds.processCSV(MAPPING_FILE, MAPPING_DELIMITER, skipMappingHeader);
			
			CSVReader patientMappingReader = ds.processCSV(PATIENT_MAPPING_FILE, MAPPING_DELIMITER, 1);
			
			CsvToI2b2TMMapping mappingClass = new CsvToI2b2TMMapping("CsvToI2b2TMMapping");
			
			List<CsvToI2b2TMMapping> mappings = mappingClass.processMapping(mappingReader);			
			
			Map<String, List<PatientMapping>> patientMappings = new HashMap<String, List<PatientMapping>>();
			
			if (patientMappingReader != null) {
				
				patientMappings = PatientMapping.class.newInstance().processMapping(patientMappingReader);			
			
			}
			/*
			 * iterate over keyset in mapping file
			*	Each key represents the filename for mapping entries
			*
			*/
			Set<String> fileNames = CsvToI2b2TMMapping.getFileNames(mappings);
						
			for(String key: fileNames){
				
				//System.err.println("Files Processed: " + totalFiles);			
				this.totalFiles++;
								
					CSVReader dataReader;
					try {
						
						if(new File(DATA_DIR + key).canRead()) {
							dataReader = ds.processCSV(DATA_DIR + key, DELIMITER, skipDataHeader);
						
							List<String[]> dataList = dataReader.readAll();

							entities.addAll(processEntities(dataList, mappings,key));
						}
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			
			}
		
			Set<Entity> patEntities = buildPatientEntities(EXPORT_TABLES, patientMappings);
			
			ETLFileWriter.WRITE_COMPLETED_DIR = this.WRITE_COMPLETED_DIR;
			
			ETLFileWriter.WRITE_PROCESSING_DIR = this.WRITE_PROCESSING_DIR;
			
			entities.addAll(thisFillTree(entities));
			
			ETLFileWriter.class.newInstance().writeToFile(entities, true);
			
			ETLFileWriter.class.newInstance().writeToFile(patEntities,true);
				
			// move processing files to completed
			try(DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(WRITE_PROCESSING_DIR))){
				
				for(Path file: stream){
												
					Files.move(file, Paths.get(WRITE_COMPLETED_DIR + file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
					
				}
				
			} catch(IOException | DirectoryIteratorException e) {
				e.printStackTrace();
				
			}
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	private Collection<? extends Entity> thisFillTree(Set<Entity> entities) throws CloneNotSupportedException {
		List<I2B2> i2b2 = new ArrayList<I2B2>();
		// fill in tree
		if(EXPORT_TABLES.contains("I2B2")){
		
			for(Entity entity: entities){
				
				if(entity instanceof I2B2){
					
					i2b2.add((I2B2) entity);
					
				}
				
			}
			
		}
		
		return I2B2.fillTree(i2b2);
	}

	private List<Entity> processEntities(List<String[]> dataList,
			List<CsvToI2b2TMMapping> mappings, String fileName) {
		
		List<Entity> builtEnts = new ArrayList<Entity>();

		List<CsvToI2b2TMMapping> list = new ArrayList<CsvToI2b2TMMapping>();
		
		for(CsvToI2b2TMMapping mapping:mappings){

			if(mapping.getFileName().equals(fileName)){

				list.add(mapping);
				
			}
			
		}
		// iterate over entities to be generated
	
		//System.out.println("Processing " + fileName + " with " + dataList.stream().count() + " lines");
		
		long startTime = System.currentTimeMillis();
		
		for(String[] data: dataList){

			if(list.size() == data.length){
				list.forEach(mapping -> {
					
					// If you need to build more entities add them here.  Logic should go into the entity's 
					// class as a constructor.  Then simply add that to the entity set.
								
					DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));

					if(!mapping.getDataLabel().equalsIgnoreCase("OMIT")){
						
						Set<Entity> newEnts = null;
						
						try {
							
							newEnts = dt.generateTables(data, mapping, buildEntities());
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						if(newEnts != null && newEnts.size() > 0){
						
							builtEnts.addAll(newEnts);
						
						}
					}							

				});
				
			}

			
			
		}
		
		return builtEnts;
	}

	private List<Entity> buildEntities() throws InstantiationException, IllegalAccessException {
		
		return Entity.buildEntityList(ENTITY_PACKAGE, EXPORT_TABLES);
		
	}
	
	public static Set<Entity> buildPatientEntities(List<String> exportTables, Map<String, List<PatientMapping>> patientMappings) throws FileNotFoundException, IOException {

		Set<Entity> entities = new HashSet<Entity>();
		
		Map<String, PatientDimension> patients = new HashMap<String,PatientDimension>();
		
		Map<String,CSVReader> csvReaders = new HashMap<String,CSVReader>();
		
		patientMappings.forEach((k,v) ->{
			
			v.stream().forEach(pm -> {
				
				CSVReader reader;
				try {
					reader = new CSVDataSource("CSVFILE").processCSV(DATA_DIR + pm.getFileName(), DELIMITER, skipDataHeader);
					
					csvReaders.put(pm.getFileName(), reader);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
				
			});			
			
		});
		
		csvReaders.forEach((fileName,reader) ->{
			try{

				List<String[]> data = reader.readAll();
				int headerSkip = 0;
				for(String[] arr: data){

					if(arr.length > 1 && arr[0].charAt(0) != '#'){
						if(headerSkip==0) headerSkip++;
						else
						patientMappings.forEach((k,v) ->{
							
							v.stream().forEach(pm -> {
								if(pm.getFileName().equals(fileName)){
									try{
										String patId = arr[ new Integer(pm.getPatId()) -1 ];
										
										if(patients.containsKey(patId)){
											
											PatientDimension pd = patients.get(patId);
											
											if(!pm.getAgeCol().isEmpty()){
												pd.setAgeInYearsNum(arr[ new Integer(pm.getAgeCol()) -1  ]);
											}
											if(!pm.getDobCol().isEmpty()){
												pd.setBirthDate(arr[ new Integer(pm.getDobCol()) -1 ]);
											}
											if(!pm.getDodCol().isEmpty()){
												pd.setDeathDate(arr[new Integer(pm.getDodCol()) -1 ]);
											}
											if(!pm.getRaceCol().isEmpty()){
												pd.setRaceCD(arr[new Integer(pm.getRaceCol()) -1 ]);
											}
											if(!pm.getSexCol().isEmpty()){
												pd.setSexCD(arr[new Integer(pm.getSexCol()) -1 ]);
											}
		
											patients.put(patId, pd);
											
										} else {
											
											PatientDimension pd = new PatientDimension("PatientDimension");
											
											pd.setPatientNum(patId);
											
											if(!pm.getAgeCol().isEmpty()){
												pd.setAgeInYearsNum(arr[ new Integer(pm.getAgeCol()) -1  ]);
											}
											if(!pm.getDobCol().isEmpty()){
												pd.setBirthDate(arr[ new Integer(pm.getDobCol()) -1 ]);
											}
											if(!pm.getDodCol().isEmpty()){
												pd.setDeathDate(arr[new Integer(pm.getDodCol()) -1 ]);
											}
											if(!pm.getRaceCol().isEmpty()){
												pd.setRaceCD(arr[new Integer(pm.getRaceCol()) -1 ]);
											}
											if(!pm.getSexCol().isEmpty()){
												pd.setSexCD(arr[new Integer(pm.getSexCol()) -1 ]);
											}
		
											patients.put(patId, pd);
											
										}
									} catch (Exception e){
										e.printStackTrace();
									}
								}
								
							});
						});
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
		patients.values().stream().forEach(pd ->{
			entities.add(pd);
		});

		
		return entities;
	}
	
}
