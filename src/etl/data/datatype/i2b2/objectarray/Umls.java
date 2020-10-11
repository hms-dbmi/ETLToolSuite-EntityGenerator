package etl.data.datatype.i2b2.objectarray;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.data.datatype.i2b2.Objectarray;
import etl.job.entity.hpds.AllConcepts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.mappings.Mapping;

public class Umls extends Objectarray {
	
	private static String DEFAULT_VALUETYPE = "T";
	
	private static String DEFAULT_NVAL = "E";	
	
	private static String UMLS_FILE = "";
	
	private static Map<String, Set<String>> umls = new HashMap<String, Set<String>>();
	
	public Umls(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Set<AllConcepts> generateTables(Mapping mapping, List<AllConcepts> entities, List<Object> values,
			List<Object> relationalValues) throws Exception {
		
		if(mapping == null || values == null || values.isEmpty()) {
			
			return new HashSet<AllConcepts>();
			
		}
		
		Set<AllConcepts> ents = new HashSet<AllConcepts>();
		
		Map<String, String> options = mapping.buildOptions(mapping);
		// comes after root node.  
		
		if(!options.containsKey("UMLSFIELD") || !options.containsKey("UMLSFILE")){
			
			throw Exception.class.newInstance();
			
		}
		// Initialize UMLS

		if(!UMLS_FILE.equals(options.get("UMLSFILE")) || umls.isEmpty()){
		
			UMLS_FILE = options.get("UMLSFILE");
			
			umls = new HashMap<String, Set<String>>();
			
			if(options.containsKey("UMLSCODE")){
				
				umls = buildUmls(options.get("UMLSFILE"), options.get("UMLSCODE"));
				
			} else if(options.containsKey("NODEREMOVE")) {
				
				umls = buildUmls(options.get("UMLSFILE"),options);
				
			} else {
			
				umls = buildUmls(options.get("UMLSFILE"));

			}
			
		} 	
		
		String umlsField = options.get("UMLSFIELD");
	
		Map<String, String> valMap = options.containsKey("VALUEMAP") ? makeKVPair(options.get("VALUEMAP"),"\\|","="): new HashMap<String, String>();
		
		Map<String, String> labels = options.containsKey("PATHPREFIXLABEL") ? makeKVPair(options.get("PATHPREFIXLABEL"),"\\|","="): new HashMap<String, String>();

		for(Object v: values) {
			/*if(v instanceof List) {
				List list = (ArrayList) v;
				
				for(Object l : list) {*/
					
			if(!(v instanceof Map)) {
				
				continue;
				
			}
			
			Map<String,Object> vmap = (HashMap<String,Object>) v;
			
			String pathPrefix = vmap.containsKey(options.get("PATHPREFIXFIELD")) ? labels.get(vmap.get(options.get("PATHPREFIXFIELD")).toString()): "";
					
			if(vmap.containsKey(options.get("IGNOREFIELD"))) {
				if(options.containsKey("IGNORETYPE")) {
					String ignoreVal = options.get("IGNORETYPE");
					String recordIgnoreField = vmap.get(options.get("IGNOREFIELD")).toString();
					
					String vmaplabel = vmap.containsKey("label") ? vmap.get("label").toString() : "";
	
					if(ignoreVal.equalsIgnoreCase(recordIgnoreField)) {

						continue;
					}
				}
			}
			
			if(vmap.containsKey(umlsField)) {
												
				String val = vmap.get(options.get("VALUEFIELD")).toString();
				
				val = valMap.containsKey(val) ? valMap.get(val): val;
				
				val = (val != null ) ? val.toString().replaceAll("[*|\\\\\\/<\\?%>\":]", ""): null;			
				
				Set<String> umlsPaths = umls.get((vmap.get(umlsField)));
												
				if(umlsPaths == null) continue;
				
				for(String umlsPath: umlsPaths) {
					
					for(Object relationalValue: relationalValues) {
						
						AllConcepts ac = new AllConcepts();
						
						List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), umlsPath));
						
						ac.setPatientNum(new Integer(relationalValue.toString()));
						ac.setConceptPath(buildConceptPath(pathList));
						
						ac.setTvalChar(val);
							
						ents.add(ac);
					
						
					}
				}
			}
				
			
		}
	
		return ents;
	}
	
	private Map<String, Set<String>> buildUmls(String umlsFilePath,
			Map<String, String> options) {
		
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		try {
			CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(umlsFilePath)));
			// skip header
			
			Iterator<String[]> iter = reader.iterator();
			
			String[] line;
			
			while((line = iter.next()) != null){
				
				if(map.containsKey(line[0])){
					Set list = map.get(line[0]);
					
					if(options.containsKey("NODEREMOVE")){
						
						list.add(line[1].replace(options.get("NODEREMOVE"), ""));

					}
					
					map.put(line[0], list);

				} else {
				
					Set list = new HashSet<String>();
					
					list.add(line[1].replace(options.get("NODEREMOVE"), ""));

					map.put(line[0], list);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return map;
	}

	private Map<String, Set<String>> buildUmls(String umlsFilePath, String ontName) {
		
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		/*
		try {
			CsvReader reader = new CsvReader(umlsFilePath);
			// skip header
			reader.readHeaders();
			
			while(reader.readRecord()){
				if(map.containsKey(reader.get(0))){
					Set list = map.get(reader.get(0));
					
					list.add(reader.get(1));
					
					map.put(ontName + ":" + reader.get(0), list);

				} else {
				
					Set list = new HashSet<String>();
					
					list.add(reader.get(1));

					map.put(ontName + ":" + reader.get(0), list);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/
		return map;
	
	}

	private Map<String, Set<String>> buildUmls(String umlsFilePath) {
		
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		/*
		try {
			CsvReader reader = new CsvReader(umlsFilePath);
			// skip header
			reader.readHeaders();
			
			while(reader.readRecord()){
				if(map.containsKey(reader.get(0))){
					Set list = map.get(reader.get(0));
					
					list.add(reader.get(1));
					
					map.put(reader.get(0), list);

				} else {
				
					Set list = new HashSet<String>();
					
					list.add(reader.get(1));

					map.put(reader.get(0), list);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/
		return map;
	}

	
}
