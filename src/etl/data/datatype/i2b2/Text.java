package etl.data.datatype.i2b2;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datasource.entities.json.udn.Sequence;
import etl.data.datatype.DataType;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

/**
 * 
 * Simple Text / String value.  
 * 
 * 
 * @author Tom DeSain
 *
 */

public class Text extends DataType {

	private static String DEFAULT_VALUETYPE = "T";
		
	
	public Text(String dataType) {
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
				if( v == null || v instanceof Map) {
					continue;
				}
								
				String value = v.toString();
				if(value != null){						
			
					if(options.containsKey("REGEXEDIT") && value instanceof String){
						value = value.replaceAll(options.get("REGEXEDIT"), "");
						
					}
					
					if(valMap.containsKey(value)){
						
						value = valMap.get(value);
						
					}
					AllConcepts ac = new AllConcepts();
					
					List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath(), value));
					ac.setPatientNum(new Integer(relationalValue.toString()));
					ac.setConceptPath(buildConceptPath(pathList));
					ac.setTvalChar(value);
					ents.add(ac);		
									
				}
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

	

	
}
