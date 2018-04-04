package etl.job.jobtype;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.commons.lang.StringUtils;

import com.csvreader.CsvReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import etl.data.datasource.JSONDataSource;
import etl.data.datatype.DataType;
import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.Export;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.i2b2.ExportI2B2;
import etl.job.jobtype.properties.JobProperties;
import etl.job.jsontoi2b2tm.entity.Mapping;
import static java.nio.file.StandardOpenOption.*;

public class JsonToI2b2TM extends JobType {
	
	//required configs
	private static String FILE_NAME; 
	
	private static String WRITE_DESTINATION; 
			
	private static String MAPPING_FILE; 
	
	// optional
	private static char MAPPING_DELIMITER;
	
	private static String RELATIONAL_KEY = ":fields:uuid";
	
	private static String OMISSION_KEY = ":fields:activestatus";
	
	private static String OMISSION_VALUE  = "A";
	
	private static Map<String,List<String>> OMISSIONS_MAP = new HashMap<String, List<String>>();
	{
		
		OMISSIONS_MAP.put(":fields:activestatus", Arrays.asList("A"));
		OMISSIONS_MAP.put(":fields:encoded_relation", Arrays.asList("",null));
		
	};
	
	
	/// Internal Config
	private static String FILE_TYPE = "JSONFILE";
	
	private static OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, APPEND };
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	private static final String ARRAY_FORMAT = "JSONFILE";
	
	private static List<String> EXPORT_TABLES = 
			new ArrayList<String>(Arrays.asList("ModifierDimension", "ObservationFact", "I2B2", 
					"ConceptDimension"));
	
	int inc = 100;
	
	int x = 0;
	
	int maxSize;
	
	public JsonToI2b2TM(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	/** primary process of this job.
	 * 
	 * @see etl.job.jobtype.JobType#runJob()
	 * 
	 * Loads a datafile and processes it based on the mapping file.
	 */
	@SuppressWarnings("static-access")
	@Override
	public void runJob(JobProperties jobProperties) {
		
		setVariables(jobProperties);
		
		Objectarray.ARRAY_FORMAT = this.ARRAY_FORMAT;
	
		Set<Entity> builtEnts = new HashSet<Entity>();
		
		try {		
						
			File dataFile = new File(FILE_NAME);
			
			List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_DELIMITER);

			if(dataFile.exists()){
				// Read datafile								
				List<Map<String, String>> list = buildRecordList();

				System.out.println("generating tables");
				
				maxSize = list.size() - 1;
				
				x = 0;
								
				while(maxSize >= x){
					
					Map<String, String> record = list.get(x);
					
					builtEnts.addAll(processEntities(mappingFile, record));	
					
					x++;
					
				}

				System.out.println("finished generating tables");

			} else {
				
				System.out.println("File " + dataFile + " Does Not Exist!");
			
			}
			
			builtEnts.addAll(thisFillTree(builtEnts));
			// expand on exception handling
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} finally{
			
		}

		for(Entity entity: builtEnts){

			try {
				Export.writeToFile(WRITE_DESTINATION + entity.getClass().getSimpleName() + ".csv", entity.toCsv(), WRITE_OPTIONS);
			
			} catch (IOException e) {
				
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
						
		}
		
	}
	
	// OMITS Records if omission key is given. 
	private List<Entity> processEntities(List<Mapping> mappings, Map<String, String> map) throws InstantiationException, IllegalAccessException{

		List<Entity> builtEnts = new ArrayList<Entity>();
		
		List<Entity> entities = buildEntities();
		
		boolean isOmitted = true;
		
		for(String omkey: OMISSIONS_MAP.keySet()) {
			
			if(map.get(omkey) != null) {

				String omissionVal = map.get(omkey);
				
				if(OMISSIONS_MAP.get(omkey).contains(omissionVal)) {
					
					isOmitted = false;
					
				} else {
					
					isOmitted = true;
					// once isOmitted is proved to be true break and continue method
					break;
				}
				
			}
			
		}

		if(!isOmitted){
			
			for(Mapping mapping: mappings){
				if(map.containsKey(mapping.getKey())){
					try {

						DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));
						if(!mapping.getDataType().equalsIgnoreCase("OMIT")){
	
							Set<Entity> newEnts = dt.generateTables(map, mapping, entities, RELATIONAL_KEY, OMISSION_KEY);
	
							if(newEnts != null && newEnts.size() > 0){
							
								builtEnts.addAll(newEnts);
							
							}
							
						}
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					
					}							
				}
			}
		}
		return builtEnts;		
	}
	
	private List<Entity> buildEntities() throws InstantiationException, IllegalAccessException {
		
		return Entity.buildEntityList(ENTITY_PACKAGE, EXPORT_TABLES);
		
	}

	private List<Map<String,String>> buildRecordList() throws Exception{
		
		JSONDataSource jds = new JSONDataSource(FILE_TYPE);
		
		JsonNode nodes = jds.buildObjectMap(new File(FILE_NAME));
		
		List<Map<String,String>> parent = new ArrayList<Map<String,String>>();
		
		Map<String,String> child = new HashMap<String, String>();
		
		Integer x = 0;
		
		for(JsonNode node: nodes){

			child = jds.processNodeToMap(node);

			parent.add(child);
			x++;
		}
		return parent;
	}	
	
	@SuppressWarnings("unused")
	private Map<String,Map<String,String>> buildMap() throws Exception{
		
		JSONDataSource jds = new JSONDataSource(FILE_TYPE);
		
		JsonNode nodes = jds.buildObjectMap(new File(FILE_NAME));
		
		Map<String, Map<String,String>> parent = new HashMap<String, Map<String,String>>();
		
		Map<String,String> child = new HashMap<String, String>();
		
		Integer x = 0;
		
		for(JsonNode node: nodes){
			
			child = jds.processNodeToMap(node);
		
			parent.put(x.toString(), child);
				
			x++;
									
		}
		
		return parent;
		
	}

	@Override
	public void setVariables(JobProperties jobProperties) {
		
		//required
		FILE_NAME = jobProperties.getProperty("filename"); ; 
		
		WRITE_DESTINATION = jobProperties.getProperty("writedestination"); 
				
		MAPPING_FILE = jobProperties.getProperty("mappingfile"); 
		
		//optional
		MAPPING_DELIMITER = jobProperties.containsKey("mappingdelimiter") && jobProperties.getProperty("mappingdelimiter").toCharArray().length == 1 ? 
				jobProperties.getProperty("mappingdelimiter").toCharArray()[0] : ',';
		
		RELATIONAL_KEY = jobProperties.containsKey("relationalkey") ? 
				jobProperties.getProperty("relationalkey"): RELATIONAL_KEY;
		
		OMISSION_KEY = jobProperties.containsKey("omissionkey") ? 
				jobProperties.getProperty("omissionkey"): OMISSION_KEY;
				
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
}
