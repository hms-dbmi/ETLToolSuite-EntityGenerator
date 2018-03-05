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

import com.csvreader.CsvReader;

import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;

public class Aarray extends Objectarray {
	
	private static String DEFAULT_VALUETYPE = "T";
	
	private static String DEFAULT_NVAL = "E";	
		
	private static Map<String, Set<String>> umls = new HashMap<String, Set<String>>();
	
	public Aarray(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
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
		
		Set<String> textKeys = generateKeys(map, mapping.getKey());

				
		if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey) && ( omissionKey != null || !omissionKey.isEmpty())){
							
			for(String str: textKeys){
				String[] lKey = str.split(":");			

				if(map.get(str) != null && !map.get(str).toString().isEmpty() && !str.equals(mapping.getKey()) && !omissions.contains(str.replace(mapping.getKey() + ":", ""))){
					try {

						boolean isNumeric = options.containsKey("NUMERICS") && options.get("NUMERICS").equalsIgnoreCase("true") 
								&& map.get(str).toString().matches("[-+]?\\d*\\.?\\d+") ? true : false; 
						
						for(Entity entity: entities){
						
							if(entity instanceof ObservationFact){
								
								ObservationFact of = new ObservationFact("ObservationFact");
								
								of.setPatientNum(map.get(relationalKey).toString());
								
								of.setEncounterNum(map.get(relationalKey) + str);
								
								of.setConceptCd(str);
								
								if(isNumeric){
									
									of.setValtypeCd("N");
									
									of.setTvalChar("E");
									
									of.setNvalNum(map.get(str).toString());
									
								} else {
									
									of.setValtypeCd("T");
															
									of.setTvalChar(map.get(str).toString());
								
								}
								
								of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

								ents.add(of);
														
							} else if(entity instanceof ConceptDimension){
						
								ConceptDimension cd = new ConceptDimension("ConceptDimension");
														
								cd.setConceptCd(str);
								
								List<String> pathList = 
										labels.containsKey(str.substring(str.lastIndexOf(':') + 1)) ? 
												new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), labels.get(str.substring(str.lastIndexOf(':') + 1)) )) :
												new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), str.substring(str.lastIndexOf(':') + 1), map.get(str).toString()));
								
								cd.setConceptPath(Entity.buildConceptPath(pathList));
								
								cd.setNameChar(map.get(str).toString());
								
								cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

								ents.add(cd);
								
							} else if(entity instanceof I2B2){
								
								List<String> pathList = new ArrayList<>();

								I2B2 i2b2 = new I2B2("I2B2");

								if(isNumeric){
									pathList = 
											labels.containsKey(str.substring(str.lastIndexOf(':') + 1)) ? 
													new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), labels.get(str.substring(str.lastIndexOf(':') + 1)) )) :
													new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), str.substring(str.lastIndexOf(':') + 1)));
																				
									i2b2.setcMetaDataXML(C_METADATAXML);
								
								} else {
									pathList = 
										labels.containsKey(str.substring(str.lastIndexOf(':') + 1)) ? 
												new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), labels.get(str.substring(str.lastIndexOf(':') + 1)), map.get(str).toString() )) :
												new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), str.substring(str.lastIndexOf(':') + 1), map.get(str).toString()));
											
								}
								
								i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList)).toString());
								
								i2b2.setcFullName(Entity.buildConceptPath(pathList));
								
								i2b2.setcName(map.get(str).toString());
								
								i2b2.setcBaseCode(mapping.getKey());
								
								i2b2.setcVisualAttributes("LA");
								
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
