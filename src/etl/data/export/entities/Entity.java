package etl.data.export.entities;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
import etl.data.datasource.CSVDataSource;
import etl.data.export.ReportWriter;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.data.export.entities.i2b2.PatientDimension;
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.PatientMapping;

public abstract class Entity implements Cloneable{
	// temp concept_cd id
	
	private String entityType = ""; 
	
	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public static int CONCEPT_CD_SEQ = 1;
	
	private enum VALID_TYPES{ PatientTrial, PatientDimension, ObservationFact, ConceptDimension, I2B2, ModifierDimension, ConceptCounts, ObjectMapping, PatientMapping, TableAccess};
		
	public static String ROOT_NODE;
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	public static Charset encoding = StandardCharsets.UTF_8;
	
	public static String SOURCESYSTEM_CD = "";
	
	public String schema;  // for generation of ctl and scripting files.
	
	private static final String DATA_DIR = "/Users/tom/Documents/Cardia/"; 
	
	public abstract void buildEntity(String str, CsvToI2b2TMMapping mapping, String[] data);
	
	// Use this method to validate a record.  
	public abstract boolean isValid();
	
	public abstract String toCsv();

	public List<Integer> buildHashIndex(Collection<Entity> entities) {
		List<Integer> list = new ArrayList<Integer>();
		
		for(Entity entity: entities){
			list.add(entity.hashCode());
		}
		
		return list;
	}
	
	public Entity(String str) throws Exception{

		if(isValidType(str)){
			setEntityType(str);
			
		} else {
		
			throw new Exception();
		
		};
	}
	
	public Entity() {
		// TODO Auto-generated constructor stub
	}

	@SuppressWarnings("finally")
	public static Entity initEntityType(String entityPackage, String entityType){
		
		Entity newInstance = null;
		
		try {		
			
			if(isValidType(entityType)){
				
				Class<?> resourceInterfaceClass = Class.forName(entityPackage + entityType);
	
				newInstance =  (Entity) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(entityType);
				
			} else {
				
				throw new Exception();
				
			}
			
		} catch (SecurityException e) {
			
			System.out.println(e);
			e.printStackTrace();
			return null;
			
		} catch (InstantiationException e) {
			
			System.out.println(e);
			e.printStackTrace();
			return null;
			
		} catch (IllegalAccessException e) {
			
			System.out.println(e);
			e.printStackTrace();
			return null;
			
		} catch (ClassNotFoundException e) {
			
			System.out.println(e);
			e.printStackTrace();
			return null;
			
		} catch (Exception e) {
			
			System.out.println(e);
			e.printStackTrace();
			return null;
			
		} finally {
			
			return newInstance;
		}
		
	}
	
	@Deprecated
	public static Set<Entity> buildPatientEntities(List<String> exportTables, Map<String, List<PatientMapping>> patientMappings) throws FileNotFoundException, IOException {
				
		Set<Entity> entities = new HashSet<Entity>();
		
		Map<String, PatientDimension> patients = new HashMap<String,PatientDimension>();
		
		Map<String,CSVReader> csvReaders = new HashMap<String,CSVReader>();
		
		patientMappings.forEach((k,v) ->{
			
			v.stream().forEach(pm -> {
				
				CSVReader reader;
				try {
					reader = new CSVDataSource("CSVFILE").processCSV(DATA_DIR + pm.getFileName(), '\t', 0);
					
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
										
										PatientDimension pd = new PatientDimension("PatientDimension");
										
										if(patients.containsKey(patId)){
											
											pd = patients.get(patId);
											
											pd.setAgeInYearsNum(pm.getAgeCol().isEmpty() ? null: arr[ new Integer(pm.getAgeCol()) -1  ]);
											
											pd.setBirthDate(pm.getDobCol().isEmpty() ? null: arr[ new Integer(pm.getDobCol()) -1 ]);
											pd.setDeathDate(pm.getDodCol().isEmpty() ? null: arr[new Integer(pm.getDodCol()) -1 ]);;
											pd.setRaceCD(pm.getRaceCol().isEmpty() ? null: arr[new Integer(pm.getRaceCol()) -1 ]);
											pd.setSexCD(pm.getSexCol().isEmpty() ? null: arr[new Integer(pm.getSexCol()) -1 ]);
											
											patients.put(patId, pd);
											
										} else {
											
											pd.setPatientNum(patId);
											pd.setAgeInYearsNum(pm.getAgeCol().isEmpty() ? null: arr[ new Integer(pm.getAgeCol())  -1 ]);
											
											pd.setBirthDate(pm.getDobCol().isEmpty() ? null: arr[ new Integer(pm.getDobCol()) -1 ]);
											pd.setDeathDate(pm.getDodCol().isEmpty() ? null: arr[new Integer(pm.getDodCol()) -1 ]);;
											pd.setRaceCD(pm.getRaceCol().isEmpty() ? null: arr[new Integer(pm.getRaceCol()) -1 ]);
											pd.setSexCD(pm.getSexCol().isEmpty() ? null: arr[new Integer(pm.getSexCol()) -1 ]);
											
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
	
	// This will return a unique collection of entities that can be processed only one file will be processed at a time.
	@Deprecated
	public static Set<Entity> buildEntities(List<String> exportTables,
			List<String[]> dataFile, String header, List<CsvToI2b2TMMapping> list) throws IOException, InstantiationException, IllegalAccessException {

		Set<Entity> entities = new HashSet<Entity>();

		// iterate over entities to be generated
	
		//System.out.println("Processing " + list.get(0).getFileName() + " with " + dataFile.stream().count() + " lines");
		
		long startTime = System.currentTimeMillis();
		
		//ReportWriter.class.newInstance().writeToFile(list.get(0).getFileName() + " \n\t" + header + "\n","BadData");
		// process each line of data
		//dataFile.stream().forEach(data -> 
		for(String[] data: dataFile){
			// use mapping file to generate entity and add it 
			if(data.length > 0 && data[0].charAt(0) != '#'){
				
				if(list.size() == data.length){

					list.forEach(mapping -> {
	
							for(String table: exportTables){
								// If you need to build more entities add them here.  Logic should go into the entity's 
								// class as a constructor.  Then simply add that to the entity set.
								if(!table.equalsIgnoreCase("PatientDimension")){
									
									Entity entity = Entity.initEntityType(ENTITY_PACKAGE, table);
									
									entity.buildEntity(table, mapping, data);
			
									
									
									if(entity.isValid()){
										//if(entity instanceof ObservationFact)
										entities.add(entity);
										
									}
							
									
								}
							}
	
					});
					
				} else {
					
					String badData = "";
					
					for(String str: data){
						
						badData += str + "\t";
						
					}
					
						//ReportWriter.class.newInstance().writeToFile("\t" + badData + "\n","BadData");
					
				}
			}
		}
			//});
		long stopTime = System.currentTimeMillis();
		
	    long elapsedTime = stopTime - startTime;
	      //System.out.println("File Entity Build Time: " + elapsedTime);
		return entities;
	
	}
	
	private static boolean isValidType(String str){
		
		for(VALID_TYPES v: VALID_TYPES.values()){
			if(v.toString().equals(str)) {
				return true;
			}
		
		}
		
		return false;
	}

	public static List<Entity> buildEntityList(String entityPackage, List<String> exportTables) {
		
		List<Entity> entities = new ArrayList<Entity>();
		
		for(String table : exportTables){
			entities.add(initEntityType(entityPackage, table));
			
		}
		
		return entities;
	}
	
	public static String buildConceptPath(List<String> pathList) {
		String path = "\\";
		for(String s: pathList){ 
			if(s != null && !s.isEmpty()){
				
				if(s.startsWith("\\")){
					
					s = s.substring(1);
					
				} if ( s.endsWith("\\")){
					
					s= s.substring(0, s.lastIndexOf("\\") - 0);
					
				}
				
				path = path + s + "\\";
			}
		}
		return path;
	}
	
	public static Integer calculateHlevel(String path){
		
		return StringUtils.countMatches(path, "\\") - 2;
		
	}	

	public String makeStringSafe(String string){
		
		if(string != null && !string.isEmpty()){
			if(string != null && !string.isEmpty() && string.substring(0, 1).equals("`")){
				return string.replaceAll("\\s{2,}", " ");
			} else {
				return "`" + string.replaceAll("\\s{2,}", " ") + "`";
			}
		}
		// return empty string not null
		return ""; 
	}
	
	public void doCsvOutput(String destination, Set<Entity> ents) throws IOException{
		Path destPath = Paths.get(destination);
		// this should use an iterator for the Entity object
		Set<String> obj = new HashSet<String>();
		
	}
	
	public Map<String,String> makeKVPair(String str, String delimiter, String kvdelimiter){
		Map<String, String> map = new HashMap<String, String>();
		for(String str2: str.split(delimiter)){
		
			String[] split = str2.split(kvdelimiter);
			
			if(split.length == 2){
				
				map.put(split[0], split[1]);
			
			}			
		}
				
		return map;
	
	}
}
