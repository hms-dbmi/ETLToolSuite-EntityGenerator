package etl.data.datatype.i2b2.objectarray;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.csvreader.CsvReader;

import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;

public class Keyvalue extends Objectarray {
	
	private static String DEFAULT_VALUETYPE = "T";
	
	private static String DEFAULT_NVAL = "E";	
		
	private static Map<String, Set<String>> umls = new HashMap<String, Set<String>>();
	
	public Keyvalue(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}
	@Override
	public  Set<Entity> generateTables(Mapping mapping, List<Entity> entities, List<Object> values,
			List<Object> relationalValues) throws Exception{
		
		Set<Entity> ents = new HashSet<Entity>();
		
		Map<String, String> options = mapping.buildOptions(mapping);

		Map<String, String> labels = makeKVPair(options.get("LABEL"),"\\|","=");
		
		Set<String> omissions = options.containsKey("OMIT") ? mapping.buildOptions(options.get("OMIT"), "\\|"): new HashSet<String>();
		
		String encKey = options.containsKey("ENCKEY") ? options.get("ENCKEY"): "";
		
		if(values == null) return new HashSet<Entity>();
		
			for(Object v: values) {
				System.out.println(v);
			
			if(!(v instanceof Map)) {
				
				continue;
				
			}
			
			Map<String,Object> vmap = (HashMap<String,Object>) v;
	
			Object encounter = vmap.containsKey(encKey) ? vmap.get(encKey): "";
			
			for(Object relationalvalue: relationalValues) {
				
				for(String label: labels.keySet()) {
					
					if(vmap.containsKey(label)) {
						
						List<Object> vals = new ArrayList(Arrays.asList(vmap.get(label)));

						if(vals.get(0) instanceof List) vals = (ArrayList<Object>) vals.get(0);

						for(Object val:vals) {
							if(val instanceof List) val = new ArrayList(Arrays.asList(((ArrayList) val).get(0)));
							if(val == null) continue; //System.out.println(label + ':' + val);
							
							String value = val.toString();
							
							boolean isNumeric = options.containsKey("NUMERICS") && options.get("NUMERICS").equalsIgnoreCase("true") 
									&& value.matches("[-+]?\\d*\\.?\\d+") ? true : false; 
							
							String str = mapping.getKey();

							label = labels.containsKey(label) ? labels.get(label): label;
							
							for(Entity entity: entities){
								
								if(entity instanceof ObservationFact){
									
									
									
									ObservationFact of = new ObservationFact("ObservationFact");
									
									of.setPatientNum(relationalvalue.toString());
									
									of.setEncounterNum(relationalvalue.toString() + ":" + str + ":" + encounter);
									
									if(isNumeric){
										
										of.setConceptCd(str + ':' + label);
										
										of.setValtypeCd("N");
										
										of.setTvalChar("E");
										
										of.setNvalNum(value);
										
									} else {
										
										of.setConceptCd(str + ":" + label + ':' + value);
										
										of.setValtypeCd("T");
																
										of.setTvalChar(value);
									
									}
									
									of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
	
									ents.add(of);
															
								} else if(entity instanceof ConceptDimension){
							
									ConceptDimension cd = new ConceptDimension("ConceptDimension");
									
									List<String> pathList = new ArrayList<String>();
																				
									String path = "";
									
									String cName = "";
									
									
									if(isNumeric){
										
										cd.setConceptCd(str + ':' + label);
										
										pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label);
										
										path = Entity.buildConceptPath(pathList);
	
										String node = label;
	
										if(node != null && node.contains("\\")){
	
											cName = node.substring(node.lastIndexOf('\\') + 1);
											
										} else {
	
											cName = node != null ? node: null;
										
										}
									
										cd.setNameChar(cName);
	
										
									} else {
										
										cd.setConceptCd(str + ":" + label + ':' + value);
	
										pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label, value);
										
										cd.setNameChar(value);
										
									}
									
				 
									cd.setConceptPath(Entity.buildConceptPath(pathList));
									
									cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
	
									ents.add(cd);
									
								} else if(entity instanceof I2B2){
									
									List<String> pathList = new ArrayList<>();
	
									String path = "";
									
									String cName = "";
									
									I2B2 i2b2 = new I2B2("I2B2");
									
									if(isNumeric){
										
										pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label);
										
										path = Entity.buildConceptPath(pathList);
	
										String node = label;
	
										if(node != null && node.contains("\\")){
	
											cName = node.substring(node.lastIndexOf('\\') + 1);
											
										} else {
											
											cName = node != null ? node :null;
										
										}
																			
										i2b2.setcMetaDataXML(C_METADATAXML);
										
										i2b2.setcName(cName);
									
									} else {
										
										pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label, value);
										
										i2b2.setcName(value);
										
									}
	
									i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList)).toString());
									
									i2b2.setcFullName(Entity.buildConceptPath(pathList));
									
									i2b2.setcBaseCode(mapping.getKey());
									
									i2b2.setcVisualAttributes("LA");
									
									i2b2.setcFactTableColumn("CONCEPT_CD");
									
									i2b2.setcTableName("CONCEPT_DIMENSION");
									
									i2b2.setcColumnName("CONCEPT_PATH");
									
									i2b2.setcColumnDataType("T");
									
									i2b2.setcOperator("LIKE");
									
									i2b2.setcDimCode(Entity.buildConceptPath(pathList));
									
									i2b2.setcToolTip(Entity.buildConceptPath(pathList));
									
									i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
									
									i2b2.setmAppliedPath("@");
									
									ents.add(i2b2);
									
								}
							}
						}
					}
		
				}
			}
		}
		
		return ents;
	}
	@Override
	public Set<Entity> generateTables(Map map, Mapping mapping,
			List<Entity> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException, Exception {
		int x= 0;
		x++;
		// options
		Map<String, String> options = mapping.buildOptions(mapping);

		Map<String, String> labels = makeKVPair(options.get("LABEL"),"\\|","=");
		
		Set<String> omissions = new HashSet<String>();
		
		if(options.containsKey("OMIT")){
			
			omissions = mapping.buildOptions(options.get("OMIT"), "\\|");
		
		}
		
		Set<Entity> ents = new HashSet<Entity>();
				
		List<Map<String, List<String>>> m = (generateArray(map.get(mapping.getKey())));
		
		String str = mapping.getKey();
		
		if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey) ){
		
				for(Map<String, List<String>> mmm : m){
					
					for(String label2: mmm.keySet()){
						
						String encKey = options.containsKey("ENCKEY") ? options.get("ENCKEY"): "";

						for(String value: mmm.get(label2)){
							
							String label = labels.containsKey(label2) ? labels.get(label2): label2;
							
							if(map.get(str) != null && !map.get(str).toString().isEmpty()) { //&& ( map.get(str).toString().trim().charAt(0) != '{' ) && !str.equals(mapping.getKey()) && !omissions.contains(str.replace(mapping.getKey() + ":", ""))){
								
								try {
									
									boolean isNumeric = options.containsKey("NUMERICS") && options.get("NUMERICS").equalsIgnoreCase("true") 
											&& value.matches("[-+]?\\d*\\.?\\d+") ? true : false; 
									
									for(Entity entity: entities){
									
										if(entity instanceof ObservationFact){
											
											ObservationFact of = new ObservationFact("ObservationFact");
											
											of.setPatientNum(map.get(relationalKey).toString());
											
											of.setEncounterNum(map.get(relationalKey) + ":" + str + ":" + mmm.get(encKey));
											
											if(isNumeric){
												
												of.setConceptCd(str + ':' + label);
												
												of.setValtypeCd("N");
												
												of.setTvalChar("E");
												
												of.setNvalNum(value);
												
											} else {
												
												of.setConceptCd(str + ":" + label + ':' + value);
												
												of.setValtypeCd("T");
																		
												of.setTvalChar(value);
											
											}
											
											of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

											ents.add(of);
																	
										} else if(entity instanceof ConceptDimension){
									
											ConceptDimension cd = new ConceptDimension("ConceptDimension");
											
											List<String> pathList = new ArrayList<String>();
																						
											String path = "";
											
											String cName = "";
											
											
											if(isNumeric){
												
												cd.setConceptCd(str + ':' + label);
												
												pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label);
												
												path = Entity.buildConceptPath(pathList);

												String node = label;

												if(node != null && node.contains("\\")){

													cName = node.substring(node.lastIndexOf('\\') + 1);
													
												} else {

													cName = node != null ? node: null;
												
												}
											
												cd.setNameChar(cName);

												
											} else {
												
												cd.setConceptCd(str + ":" + label + ':' + value);

												pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label, value);
												
												cd.setNameChar(value);
												
											}
											
						 
											cd.setConceptPath(Entity.buildConceptPath(pathList));
											
											cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

											ents.add(cd);
											
										} else if(entity instanceof I2B2){
											
											List<String> pathList = new ArrayList<>();

											String path = "";
											
											String cName = "";
											
											I2B2 i2b2 = new I2B2("I2B2");
											
											if(isNumeric){
												
												pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label);
												
												path = Entity.buildConceptPath(pathList);

												String node = label;

												if(node != null && node.contains("\\")){

													cName = node.substring(node.lastIndexOf('\\') + 1);
													
												} else {
													
													cName = node != null ? node :null;
												
												}
																					
												i2b2.setcMetaDataXML(C_METADATAXML);
												
												i2b2.setcName(cName);
											
											} else {
												
												pathList = Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label, value);
												
												i2b2.setcName(value);
												
											}

											i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList)).toString());
											
											i2b2.setcFullName(Entity.buildConceptPath(pathList));
											
											i2b2.setcBaseCode(mapping.getKey());
											
											i2b2.setcVisualAttributes("LA");
											
											i2b2.setcFactTableColumn("CONCEPT_CD");
											
											i2b2.setcTableName("CONCEPT_DIMENSION");
											
											i2b2.setcColumnName("CONCEPT_PATH");
											
											i2b2.setcColumnDataType("T");
											
											i2b2.setcOperator("LIKE");
											
											i2b2.setcDimCode(Entity.buildConceptPath(pathList));
											
											i2b2.setcToolTip(Entity.buildConceptPath(pathList));
											
											i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
											
											i2b2.setmAppliedPath("@");
											
											ents.add(i2b2);
											
										}
									}
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								
							}
													
						}
						
					}
				
				}
			
		}
		return ents;
	
	}



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
}
