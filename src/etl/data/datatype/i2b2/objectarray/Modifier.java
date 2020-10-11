package etl.data.datatype.i2b2.objectarray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datatype.i2b2.Objectarray;
import etl.job.entity.hpds.AllConcepts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.mappings.Mapping;


public class Modifier extends Objectarray {

	public Modifier(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}
	@Override
	public  Set<AllConcepts> generateTables(Mapping mapping, List<AllConcepts> entities, List<Object> values,
			List<Object> relationalValues) throws Exception{
		
		Set<AllConcepts> ents = new HashSet<AllConcepts>();
		
		Map<String, String> options = mapping.buildOptions(mapping);
		
		Map<String, String> labels = makeKVPair(options.get("MODLABEL"),"\\|","=");

		List<String> modRequiredNode = Arrays.asList(options.get("MODIFIERVALUE").split("\\|"));
		
		Set<String> textKeys = options.containsKey("MODIFIERS") ? generateKeys(options.get("MODIFIERS"), mapping.getKey()): new HashSet<String>();
		
		if(values == null) return new HashSet<AllConcepts>();
		
		for(Object v: values) {
			
			if(!(v instanceof Map)) {
				
				continue;
				
			}
			
			Map<String,Object> vmap = (HashMap<String,Object>) v;
			
			Boolean hasValueField = false;
			String reqNode = "";
			// make sure record has valuefield needed to display
			String conceptNode = new String();
			String currReqVal = new String();
			for(String reqNode2:modRequiredNode) {
				
				hasValueField = vmap.containsKey(reqNode2) ? true: false;
				
				if(hasValueField) {
					
					reqNode = vmap.get(reqNode2).toString();
					
					conceptNode = reqNode2;
					
					currReqVal = vmap.get(reqNode2).toString();
					
					break;	
				}
			
			}
			if(labels.containsKey(conceptNode)) conceptNode = labels.get(conceptNode);
			// skip record if missing required field
			if(!hasValueField) continue;
			
			for(Object relationalvalue: relationalValues) {
				
				for(String label: labels.keySet()) {
				
					if(vmap.containsKey(label)) {
						
						List<Object> vals = new ArrayList(Arrays.asList(vmap.get(label)));
						
						for(Object val:vals) {
							
							if(val == null) continue; //System.out.println(label + ':' + val);
							
							val = (val != null ) ? val.toString().replaceAll("[*|\\\\\\/<\\?%>\":]", ""): null;			
							
							String value = val.toString();
							
							boolean isNumeric = value.matches("[-+]?\\d*\\.?\\d+") ? true : false; 
							
							label = labels.containsKey(label) ? labels.get(label): label;
							
							label = label.trim();
							
							value = value.trim();
							
							AllConcepts ac = new AllConcepts();

							ac.setPatientNum(new Integer(relationalvalue.toString()));

							if(isNumeric) {
								
								List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath(),label));
								ac.setConceptPath(buildConceptPath(pathList));

								ac.setNvalNum(value);
								
							} else {
								
								List<String> pathList = new ArrayList<>(Arrays.asList( mapping.getRootNode(), mapping.getSupPath(), label, value));
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
	
	private String getModValueNode(String string,Map map) {
		String[] strarr = string.split("\\|");
		
		for (String string2 : strarr) {
			if(map.get(string2) != null && !map.get(string2).toString().isEmpty()){
				return string2;
			}
		}
		
		return null;
	}


	private int findIndexOf(char[] carr, char ch, int indexof){
		int index = 0;
		int y = 1;
		
		if(indexof >= y){
			
			for(char c: carr){
				
				if(c == ch){
					
					if(indexof == y){
						
						return index;
						
					} else {
						
						y++;
					}
				}
				
				index++;
			}
		}
		
		return -1;
	}
}
