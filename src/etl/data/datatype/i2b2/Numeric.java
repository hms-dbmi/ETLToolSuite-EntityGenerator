package etl.data.datatype.i2b2;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import etl.data.datatype.DataType;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.mappings.Mapping;

public class Numeric extends DataType{

	private static String DEFAULT_VALUETYPE = "N";
			
	public Numeric(String dataType) {
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
		
		Map<String, String> valMap = options.containsKey("VALUEMAP") ? makeKVPair(options.get("VALUEMAP"),"\\|","="): new HashMap<String, String>();
		
	
		for(Object relationalValue: relationalValues) {
	
			for(Object v: values) {
	
				if( v == null) {
					continue;
				}
				String value = v.toString();
			
				if(!StringUtils.isNumeric(value)) {
					value = value.replaceAll("[^0-9.]", "");
					if(value.isEmpty()) break; 
				} 
			
				if(options.containsKey("REGEXEDIT") && value instanceof String){
					value = value.replaceAll(options.get("REGEXEDIT"), "");
					
				}
				
				if(valMap.containsKey(value)){
					
					value = valMap.get(value);
					
				}
				
				AllConcepts ac = new AllConcepts();
				
				List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath()));
				
				ac.setPatientNum(new Integer(relationalValue.toString()));
				ac.setConceptPath(buildConceptPath(pathList));
				ac.setNvalNum(value);
				ents.add(ac);		
			}
		}
	
		
		return ents;
	}

	@Override
	public Set<AllConcepts> generateTables(Map map, Mapping mapping, List<AllConcepts> entities, String relationalKey,
			String omissionKey) throws InstantiationException, IllegalAccessException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	private String getLastNode(String string) {
		String[] split = string.split("\\\\");

		return split[split.length - 1];
		
	}

	private List<Object> findValueByKey(Map record, String[] key) throws Exception {
		
		Map record2 = new LinkedHashMap(record);
		
		Iterator<String> iter = new ArrayList(Arrays.asList(key)).iterator();
		while(iter.hasNext()) {
			
			String currKey = iter.next();
			
			if(record2.containsKey(currKey)){
				
				Object obj = record2.get(currKey);
				
				if(obj == null) {

					return new ArrayList<Object>();
					
				}
				if(obj instanceof Map) {
					
					record2 = new LinkedHashMap((LinkedHashMap)obj);
					
					// if last key return a the hashmap of values to be processed by a datatype
					if(!iter.hasNext()) {
						
						List<Object> l = new ArrayList<Object>();
						
						l.add(record2);
						
						return l;
					
					}
				
				} else if ( obj instanceof String || obj instanceof Boolean ) {

					return new ArrayList(Arrays.asList(obj));
					
				} else if ( obj instanceof List ) {
					
					List<Object> l = new ArrayList<Object>();
					
					for(Object o: (List) obj) {
						
						if(o instanceof Map) {
							
							record2 = new LinkedHashMap((LinkedHashMap) o);
							
							l.add(record2);
							
						} else {
							return (ArrayList) obj;
							
						}
						
					}

					return l;
						
				} else if ( obj != null) {
					
					System.out.println(obj.getClass());
					
				}
				/*
				try {

					DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));
					if(!mapping.getDataType().equalsIgnoreCase("OMIT")){

						Set<Entity> newEnts = dt.generateTables(record, mapping, entities, RELATIONAL_KEY, OMISSION_KEY);

						if(newEnts != null && newEnts.size() > 0){
						
							builtEnts.addAll(newEnts);
						
						}
						
					}
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				
				}							
				*/
			} else {
				
				//throw new Exception("Bad Key used to find value: " + currKey);
				
			}
			
		}
		
		return null;
	}
	

}
