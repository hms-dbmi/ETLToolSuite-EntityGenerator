package etl.data.datatype.i2b2.objectarray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datasource.JSONDataSource;
import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;

public class Valuearray extends Objectarray {
	
	private static String DEFAULT_VALUETYPE = "T";
	
	public Valuearray(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}


	/**
	 * This method is used currently for JSON only.
	 * Current strategy will be to just create generateTables for each jobType.
	 * Would like to enact a universal mapping file and Record List object.
	 * Currently next to impossible until all transforms are removed from ETL processes.
	 * Transforms exist to appease required business requirements.
	 * 
	 * - Tom D. 30/12/18
	 * @throws Exception 
	 */
	
	@SuppressWarnings("unchecked")
	public Set<Entity> generateTables(Map map, Mapping mapping, List<Entity> entities, String relationalKey, String omissionKey) throws Exception {

		Set<Entity> ents = new HashSet<Entity>();

		Map<String, String> options = mapping.buildOptions(mapping);
		
		if(map != null && map.containsKey(mapping.getKey())){ 
			
			List<Map<String, List<String>>> records = buildValueMap(map.get(mapping.getKey()).toString(),options);
			
			for(Map<String, List<String>> record: records) {
				
				// if record contains the primary key ( PRIME Key ) and valfield given in options continue 
				if(record.containsKey(options.get("PRIMEKEY")) &&	
					record.containsKey(options.get("VALFIELD"))){
					
					List<String> primeKeys = record.get(options.get("PRIMEKEY"));
					
					String primeKey = new String();
					
					if(primeKeys.size() == 0) {
						
						throw new Exception("Prime Key value empty for: " + mapping.getKey());
					
					} else if(primeKeys.size() > 1) {
						
						throw new Exception("Prime Key value exceeds 1 value: " + mapping.getKey() + " values " + primeKeys.toString());
						
					} else {
						
						primeKey = primeKeys.get(0);
					}
					
					// iterate over values
					String valField = options.get("VALFIELD");
					
					List<String> values = record.get(valField);
					
					for(String value:values) {
						
						for(Entity entity: entities) {
						
							if(entity instanceof ObservationFact){
								
								ObservationFact of = new ObservationFact("ObservationFact");
								
								of.setPatientNum(map.get(relationalKey).toString());
								
								of.setEncounterNum(map.get(relationalKey) + mapping.getKey() + primeKey);
								
								of.setConceptCd(mapping.getKey() + ':' + valField + ':' + value);
								
								of.setValtypeCd(DEFAULT_VALUETYPE);
														
								of.setTvalChar(value);
								
								of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
								
								ents.add(of);
														
							} else if(entity instanceof ConceptDimension){
						
								ConceptDimension cd = new ConceptDimension("ConceptDimension");
														
								cd.setConceptCd(mapping.getKey() + ':' + valField + ':' + value);
								
								List<String> pathList = new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), value));
								
								cd.setConceptPath(Entity.buildConceptPath(pathList));
								
								cd.setNameChar(value);
								
								cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
								
								ents.add(cd);
								
							} else if(entity instanceof I2B2){
								
								I2B2 i2b2 = new I2B2("I2B2");
								
								List<String> pathList = new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), value));
								
								i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList)).toString());
								
								i2b2.setcFullName(Entity.buildConceptPath(pathList));
								
								i2b2.setcName(value);
								
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
					
				} else {

					throw new Exception("Missing PRIMEKEY or VALFIELD in mapping file - options - for mapping key: " + mapping.getKey());
				
				}
				
			}

		} 

		return ents;

	}

	/// this can go into abstract datatype layer 
	private List<Map<String, List<String>>> buildValueMap(String string, Map<String, String> options) throws Exception {
		String format = options.containsKey("FORMAT") ? options.get("FORMAT"): null;
		
		List<Map<String, List<String>>> valMap = new ArrayList<Map<String, List<String>>>();
		
		if(format != null) {
			if(format.equalsIgnoreCase("JSON")) {
				
				valMap = buildJsonMap(string);
				
			}
			
			
		}
		
		return valMap;
	}

	private List<Map<String, List<String>>> buildJsonMap(String string) throws Exception {
		
		JSONDataSource jds = new JSONDataSource(Objectarray.ARRAY_FORMAT);
		
		List<Map<String, List<String>>> t = jds.processJsonArrayToMap(string);
		
		return t;
	
	}
}
