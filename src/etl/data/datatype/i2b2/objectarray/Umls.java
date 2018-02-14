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

import javax.sound.midi.SysexMessage;

import com.csvreader.CsvReader;

import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;

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
	public Set<Entity> generateTables(Map map, Mapping mapping,
			List<Entity> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException, Exception {
		// options
		Map<String, String> options = mapping.buildOptions(mapping);
		
		Set<Entity> ents = new HashSet<Entity>();

		// comes after root node.  
		String pathPrefixField = new String();
		String pathPrefix = new String();
		
		if(options.containsKey("PATHPREFIXFIELD")){
			
			pathPrefixField = options.get("PATHPREFIXFIELD");
			
		} else if(options.containsKey("PATHPREFIX")){
			
			pathPrefix = options.get("PATHPREFIX");
			
		}
		
		if(!options.containsKey("UMLSFIELD") || !options.containsKey("UMLSFILE")){
			
			throw Exception.class.newInstance();
			
		}
		// Initialize UMLS

		if(!UMLS_FILE.equals(options.get("UMLSFILE"))){
		
			UMLS_FILE = options.get("UMLSFILE");
			
			umls = new HashMap<String, Set<String>>();
			
		}
		
		if(umls != null && umls.size() == 0){
			
			if(options.containsKey("UMLSCODE")){
				
				umls = buildUmls(options.get("UMLSFILE"), options.get("UMLSCODE"));
				
				for(String key: umls.keySet()){
					if(key.startsWith("MIM")){
					//System.err.println(key + "=" + umls.get(key));
					}
				}
				
			} else if(options.containsKey("NODEREMOVE")) {
				
				umls = buildUmls(options.get("UMLSFILE"),options);
				
			} else {
			
				umls = buildUmls(options.get("UMLSFILE"));

			}
		} 	
		
		for(Entity entity: entities){			
			if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey)){

				List<Map<String, List<String>>> umlscodes = generateArray(map.get(mapping.getKey()));
				
				Map<String, String> labels = options.containsKey("PATHPREFIXLABEL") ? makeKVPair(options.get("PATHPREFIXLABEL"),"\\|","="): new HashMap<String, String>();
				
				Map<String, String> valMap = options.containsKey("VALUEMAP") ? makeKVPair(options.get("VALUEMAP"),"\\|","="): new HashMap<String, String>();
				
				for(Map<String, List<String>> code: umlscodes){
					
					if(!pathPrefixField.isEmpty() && code.containsKey(pathPrefixField)){
						
						pathPrefix = code.get(pathPrefixField).get(0);
						
					}
					
					if(labels.containsKey(pathPrefix)){
						
						pathPrefix = labels.get(pathPrefix);
						
					}
					
					
					if(code.containsKey(options.get("UMLSFIELD"))){
						
						Set<String> concepts = new HashSet<String>();
						
						if(umls.containsKey(code.get(options.get("UMLSFIELD")).get(0))){
														
							concepts = umls.get(code.get(options.get("UMLSFIELD")).get(0));
							
						}
						
						try {
			
							String val = code.get(options.get("VALUEFIELD")).get(0);

							if(valMap.containsKey(val)){
								
								val = valMap.get(val);
								
							}
							
							if(entity instanceof ObservationFact){
								
								ObservationFact of = new ObservationFact("ObservationFact");

								of.setPatientNum(map.get(relationalKey).toString());
								
								of.setEncounterNum(map.get(relationalKey) + mapping.getKey());
								
								of.setConceptCd(code.get(options.get("UMLSFIELD")).get(0) + ":" + pathPrefix + ":"  + val);
								
								of.setValtypeCd(DEFAULT_VALUETYPE);
								
								of.setTvalChar(val);
								
								of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

								ents.add(of);
														
							} else if(entity instanceof ConceptDimension){
								
								for(String concept: concepts){
									//concepts.forEach(concept -> {
									try {
									
										ConceptDimension cd = new ConceptDimension("ConceptDimension");
										
										cd.setConceptCd(code.get(options.get("UMLSFIELD")).get(0) + ":" + pathPrefix + ":" + val);
																				
										List<String> pathList = new ArrayList<>(Arrays.asList(mapping.getRootNode(), pathPrefix, concept, mapping.getSupPath(), val));
										
										String path = Entity.buildConceptPath(pathList);
										
										cd.setConceptPath(path);
										
										cd.setNameChar(val);
										
										cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

										ents.add(cd);										
								
									} catch (Exception e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								//);
							} else if(entity instanceof I2B2){
								
								//concepts.forEach(concept -> {
								for(String concept: concepts){
									try {	
											
										I2B2 i2b2 = new I2B2("I2B2");
																				
										List<String> pathList = new ArrayList<>(Arrays.asList(mapping.getRootNode(), pathPrefix, concept, mapping.getSupPath(), val));
										
										String path = Entity.buildConceptPath(pathList);
										
										i2b2.setcHlevel(Entity.calculateHlevel(path).toString());
										
										i2b2.setcFullName(path);
									
										i2b2.setcName(val);
										
										i2b2.setcBaseCode(code.get(options.get("UMLSFIELD")).get(0));
										
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
										
									} catch (Exception e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								//);
							}
							
						} catch (Exception e) {
							
							e.printStackTrace();
						
						}
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
			CsvReader reader = new CsvReader(umlsFilePath);
			// skip header
			reader.readHeaders();
			
			while(reader.readRecord()){
				if(map.containsKey(reader.get(0))){
					Set list = map.get(reader.get(0));
					
					if(options.containsKey("NODEREMOVE")){
						
						list.add(reader.get(1).replace(options.get("NODEREMOVE"), ""));

					}
					
					map.put(reader.get(0), list);

				} else {
				
					Set list = new HashSet<String>();
					
					list.add(reader.get(1).replace(options.get("NODEREMOVE"), ""));

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

		return map;
	}

	private Map<String, Set<String>> buildUmls(String umlsFilePath, String ontName) {
		
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		
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

		return map;
	
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return map;
	}
	
	
}
