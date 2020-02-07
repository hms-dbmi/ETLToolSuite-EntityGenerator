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
import etl.job.entity.Mapping;
import etl.job.entity.hpds.AllConcepts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.ObservationFact;

public class Keyvalue extends Objectarray {
	
	private static String DEFAULT_VALUETYPE = "T";
	
	private static String DEFAULT_NVAL = "E";	
		
	private static Map<String, Set<String>> umls = new HashMap<String, Set<String>>();
	
	public Keyvalue(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}
	@Override
	public  Set<AllConcepts> generateTables(Mapping mapping, List<AllConcepts> entities, List<Object> values,
			List<Object> relationalValues) throws Exception{
		
		Set<AllConcepts> ents = new HashSet<AllConcepts>();
		
		Map<String, String> options = mapping.buildOptions(mapping);

		Map<String, String> labels = makeKVPair(options.get("LABEL"),"\\|","=");
		
		Set<String> omissions = options.containsKey("OMIT") ? mapping.buildOptions(options.get("OMIT"), "\\|"): new HashSet<String>();
		
		boolean isAlphabatized = false; 
		if(options.containsKey("ALPHABATIZE")) {
			if(options.get("ALPHABATIZE").equalsIgnoreCase("true")) {
				isAlphabatized = true;
			}
			
		}
		
		String encKey = options.containsKey("ENCKEY") ? options.get("ENCKEY"): "";
		String[] encKeys = encKey.split("-");

		if(values == null) return new HashSet<AllConcepts>();
		
			for(Object v: values) {
				
				if(!(v instanceof Map)) {
					
					continue;
					
				}
				
				Map<String,Object> vmap = (HashMap<String,Object>) v;
		
				String enc = new String();
				for(String ek: encKeys) {
					enc = vmap.containsKey(ek) ? enc + vmap.get(ek): enc;
				}
				
				Object encounter = enc;
				for(Object relationalvalue: relationalValues) {
					
					for(String label: labels.keySet()) {
						
						if(vmap.containsKey(label)) {
							
							List<Object> vals = new ArrayList(Arrays.asList(vmap.get(label)));
	
							if(vals.get(0) instanceof List) vals = (ArrayList<Object>) vals.get(0);
	
							for(Object val:vals) {
								if(val instanceof List) val = new ArrayList(Arrays.asList(((ArrayList) val).get(0)));
								if(val == null) continue; //System.out.println(label + ':' + val);
								
								String value = val.toString();
								
								value = (value != null ) ? value.toString().replaceAll("[*|\\\\\\/<\\?%>\":]", ""): null;			

								boolean isNumeric = options.containsKey("NUMERICS") && options.get("NUMERICS").equalsIgnoreCase("true") 
										&& value.matches("[-+]?\\d*\\.?\\d+") ? true : false; 
								
								String str = mapping.getKey();
	
								if(isAlphabatized) {
									String letter = value.substring(0,1) + '\\';
									str = str + letter;
								}
								
								label = labels.containsKey(label) ? labels.get(label): label;
								
								label = label.trim();
								
								value = value.trim();
								
								AllConcepts ac = new AllConcepts();
								
								ac.setPatientNum(new Integer(relationalvalue.toString()));
								
								if(isNumeric) {
									
									List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath(), label));
									
									ac.setConceptPath(buildConceptPath(pathList));

									ac.setNvalNum(value);
									
								} else {
									
									List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath(), label,value));
									
									ac.setConceptPath(buildConceptPath(pathList));

									ac.setTvalChar(value);
									
								}
								ents.add(ac);

								
							}
						}
			
					}
				}
		}
		
		return ents;
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
		
		String str = mapping.getKey();
		
		if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey) ){
		
				for(Map<String, List<String>> mmm : m){
					
					for(String label2: mmm.keySet()){
						
						String encKey = options.containsKey("ENCKEY") ? options.get("ENCKEY"): "";

						for(String value: mmm.get(label2)){
							
							String label = labels.containsKey(label2) ? labels.get(label2): label2;
							
							if(map.get(str) != null && !map.get(str).toString().isEmpty()) { //&& ( map.get(str).toString().trim().charAt(0) != '{' ) && !str.equals(mapping.getKey()) && !omissions.contains(str.replace(mapping.getKey() + ":", ""))){
								
									
								boolean isNumeric = options.containsKey("NUMERICS") && options.get("NUMERICS").equalsIgnoreCase("true") 
										&& value.matches("[-+]?\\d*\\.?\\d+") ? true : false; 
								
								value = (value != null ) ? value.toString().replaceAll("[*|\\\\\\/<\\?%>\":]", ""): null;		
								
								String relationalvalue = map.get(relationalKey).toString();
								
								AllConcepts ac = new AllConcepts();
								
								List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath()));

								ac.setPatientNum(new Integer(relationalvalue.toString()));
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
				
				}
			
		}
		return ents;
	
	}


}
