package etl.job.jobtype;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.PatientMapping;
import utility.directoryutils.DirUtils;

public class CSVNew extends JobType {

	// Parameters to go in a config file
	private static final String DATA_DIR = "/Users/tom/Documents/JacksonMapping/load/"; 

	private static final String WRITE_PROCESSING_DIR = "/Users/tom/Documents/JacksonMapping/Processing/";
	
	private static final String WRITE_COMPLETED_DIR = "/Users/tom/Documents/JacksonMapping/Completed/"; 
	
	private static final String MAPPING_FILE = "/Users/tom/Documents/JacksonMapping/mapping.csv";
	
	private static final String PATIENT_MAPPING_FILE = "/Users/tom/Documents/JacksonMapping/PatientMapping.csv";
	
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, APPEND };
	
	private static String SKIP_MAPPER_HEADER = "N";
	
	private static String SKIP_DATA_HEADER = "N";
	
	private static final String ROOT_NODE = "Test";

	
	// Void Parameters

	private static final String CLEAN_COMMENTS = "T";
	
	private static final char DELIMITER = ','; 	

	private static final char MAPPING_DELIMITER = ',';
	
	private static final List<String> EXPORT_TABLES = new ArrayList<String>(Arrays.asList("PatientDimension", "ConceptDimension", "ObservationFact", "I2B2"));

	// class variables will remain
	public static int totalFiles = 0;
	
	private final static int skipMappingHeader = SKIP_DATA_HEADER == "Y" ? 1 : 0;;
	
	private final static int skipDataHeader = SKIP_MAPPER_HEADER == "Y" ? 1 : 0; ;
	
	private static final String EXPORT_PACKAGE = "etl.data.export.i2b2.";
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	// initializer
	{
		
		Entity.ROOT_NODE = CSVNew.ROOT_NODE;
		
	}
	
	public CSVNew(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void runJob() {
		
		// used to store entities that will be processed through this job
		Set<Entity> entities = new HashSet<Entity>();
	
		// clean processing dir
		try{
		
			DirUtils.removeAllFileFromDir(Paths.get(WRITE_PROCESSING_DIR));
		
		} catch (IOException e){
			
			System.err.println("Processing Dir could not be cleaned ");
			e.printStackTrace();
		}
		
			
		try {
			
			// need to switch this to initializer in super and use datasource type as Class name.
			CSVDataSource ds = new CSVDataSource("CSVFILE");

			CSVReader mappingReader = ds.processCSV(MAPPING_FILE, MAPPING_DELIMITER, skipMappingHeader);
			
			CSVReader patientMappingReader = ds.processCSV(PATIENT_MAPPING_FILE, MAPPING_DELIMITER, 1);
			
			CsvToI2b2TMMapping mappingClass = new CsvToI2b2TMMapping("CsvToI2b2TMMapping");
			
			List<CsvToI2b2TMMapping> mappings = mappingClass.processMapping(mappingReader);			

			Map<String, List<PatientMapping>> patientMappings = PatientMapping.class.newInstance().processMapping(patientMappingReader);			
			/*
			 * iterate over keyset in mapping file
			*	Each key represents the filename for mapping entries
			*
			*/
			Set<String> fileNames = CsvToI2b2TMMapping.getFileNames(mappings);
						
			for(String key: fileNames){
				
				System.err.println("Files Processed: " + totalFiles);			
				this.totalFiles++;
								
					CSVReader dataReader;
					try {
						
						dataReader = ds.processCSV(DATA_DIR + key, DELIMITER, skipDataHeader);
					
						List<String[]> dataList = dataReader.readAll();
						
					/*  This was a hack for cardia original load.  will delete after next load - td 1/31/18
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
						*/

						entities.addAll(processEntities(dataList, mappings,key));
						
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
	
		System.out.println("Processing " + fileName + " with " + dataList.stream().count() + " lines");
		
		long startTime = System.currentTimeMillis();
		
		for(String[] data: dataList){
			if(list.size() == data.length - 1){
				
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
