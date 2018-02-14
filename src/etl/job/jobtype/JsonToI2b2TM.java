package etl.job.jobtype;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
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
import etl.data.export.i2b2.ExportI2B2;
import etl.job.jsontoi2b2tm.entity.Mapping;
import static java.nio.file.StandardOpenOption.*;

public class JsonToI2b2TM extends JobType {
	
	// all variables will be passed arguments
	//    private static final String FILE_NAME = "/Users/tom/Documents/udn/udn_patientdump_2017-12-06.json"; 
	//private static final String FILE_NAME = "/Users/tom/Documents/udn_patientdump_2017-06-28.json";
	//private static final String FILE_NAME = "/Users/tom/Documents/udn/2017-12-2.json";
	private static final String FILE_NAME = "/Users/tom/Documents/udn/2018-01.json";

	//private static final String FILE_NAME = "/Users/tom/Documents/1patient.json";
	
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, APPEND };
	
	private static final String WRITE_DESTINATION = "/Users/tom/Documents/udn/completed/";
	
	private static final String FILE_TYPE = "JSONFILE";
		
	private static final String MAPPING_FILE = "/Users/tom/Documents/udn/udn_mapping2.csv";
	
	private static final List<String> EXPORT_TABLES = new ArrayList<String>(Arrays.asList("ModifierDimension", "ObservationFact", "I2B2", "ConceptDimension"));
			
	private static final char MAPPING_DELIMITER = ',';
	
	private static final String RELATIONAL_KEY = ":fields:uuid";
	
	private static final String OMISSION_KEY = ":fields:encoded_relation";
	
	private static final String OMISSION_VALUE = "NOT NULL OR EMPTY";

	/// Internal Config
	private static final String EXPORT_TYPE = "ExportI2B2";
	
	private static final String EXPORT_PACKAGE = "etl.data.export.i2b2.";
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	private static final String ARRAY_FORMAT = "JSONFILE";
	
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
	@Override
	public void runJob() {
		
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
					
					Map record = list.get(x);
					builtEnts.addAll(processEntities(mappingFile, record));	
					
					x++;
					
				}

				System.out.println("finished generating tables");

			} else {
				
				System.out.println("File " + dataFile + " Does Not Exist!");
			
			}	
			
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

	private List<Entity> processEntities(List<Mapping> mappings, Map<String, String> map) throws InstantiationException, IllegalAccessException{

		List<Entity> builtEnts = new ArrayList<Entity>();
		
		List<Entity> entities = buildEntities();
		
		boolean isOmitted = true;
		
		if(map.containsKey(OMISSION_KEY)){

			if( map.get(OMISSION_KEY) == null || map.get(OMISSION_KEY).isEmpty()){
			
				isOmitted = false;    
			
			}
		} else {
			
			isOmitted = false;
			
		}
		
		if(!isOmitted){
			for(Mapping mapping: mappings){
	
				if(map.containsKey(mapping.getKey())){
					
					try {
						
						//Export export = Export.initExportType(EXPORT_PACKAGE, EXPORT_TYPE);
						//System.out.println(mapping.getDataType());
						//export.generateTables(map, mapping, entities, RELATIONAL_KEY, OMISSION_KEY);
						
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
			System.err.println(child);
			parent.add(child);
			x++;
		}
		return parent;
	}	
	
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
	
}
