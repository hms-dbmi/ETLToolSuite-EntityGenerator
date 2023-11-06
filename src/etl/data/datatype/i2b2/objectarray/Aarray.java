package etl.data.datatype.i2b2.objectarray;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datatype.i2b2.Objectarray;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.mappings.Mapping;


public class Aarray extends Objectarray {
	
	private static String DEFAULT_VALUETYPE = "T";
	
	private static String DEFAULT_NVAL = "E";	
		
	private static Map<String, Set<String>> umls = new HashMap<String, Set<String>>();
	
	public Aarray(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Set<AllConcepts> generateTables(Map map, Mapping mapping,
			List<AllConcepts> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException, Exception {
		int x= 0;

		x++;
		// options
		Map<String, String> options = mapping.buildOptions(mapping);

		Map<String, String> labels = makeKVPair(options.get("LABEL"),"\\|","=");
		
		Set<String> omissions = new HashSet<String>();
		
		if(options.containsKey("OMIT")){
			
			omissions = mapping.buildOptions(options.get("OMIT"), "\\|");
		
		}
		
		Set<AllConcepts> ents = new HashSet<AllConcepts>();

		List<Map<String, List<String>>> m = (generateArray(map.get(mapping.getKey())));
		
		Set<String> textKeys = generateKeys(map, mapping.getKey());

				
		if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey) && ( omissionKey != null || !omissionKey.isEmpty())){
							
			for(String str: textKeys){
				String[] lKey = str.split(":");			

				if(map.get(str) != null && !map.get(str).toString().isEmpty() && !str.equals(mapping.getKey()) && !omissions.contains(str.replace(mapping.getKey() + ":", ""))){
	
					boolean isNumeric = options.containsKey("NUMERICS") && options.get("NUMERICS").equalsIgnoreCase("true") 
							&& map.get(str).toString().matches("[-+]?\\d*\\.?\\d+") ? true : false; 
					
					AllConcepts ac = new AllConcepts();
					
					String relationalValue = map.get(relationalKey).toString();
					
					String value = map.get(str).toString();
					
					List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath()));
					
					ac.setPatientNum(new Integer(relationalValue.toString()));
					ac.setConceptPath(buildConceptPath(pathList));
					
					if(isNumeric) {
					
						ac.setNvalNum(value);
						
					} else {
						
						ac.setTvalChar(value);
						
					}
					ents.add(ac);

				}
				
			}
			
		}

		return ents;
	
	}


	/*
	private Map<String, Set<String>> buildUmls(String umlsFilePath) {
		
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		
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
		
			e.printStackTrace();
		
		} catch (IOException e) {
		
			e.printStackTrace();
		
		}

		return map;
	
	}
	*/
}
