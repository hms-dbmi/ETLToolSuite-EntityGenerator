package etl.data.datatype.i2b2;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datatype.DataType;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;
import etl.mapping.CsvToI2b2TMMapping;

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
	/**
	 * This method is used currently for JSON only.
	 * Current strategy will be to just create generateTables for each jobType.
	 * Would like to enact a universal mapping file and Record List object.
	 * Currently next to impossible until all transforms are removed from ETL processes.
	 * Transforms exist to appease required business requirements.
	 * 
	 * Need to remove the array from this.  Create a new datatype to handle denormed data.  Starts line 69.
	 * Need to remove the entity creation from here.
	 * Build Constructors in each Entity that build object based on datatype passed.  
	 * - Tom D. 30/12/18
	 */
	@Deprecated
	@SuppressWarnings("unchecked")
	public Set<Entity> generateTables(Map map, Mapping mapping, List<Entity> entities, String relationalKey, String omissionKey) {
		Set<Entity> ents = new HashSet<Entity>();

		Map<String, String> options = mapping.buildOptions(mapping);
		
		for(Entity entity: entities){			
			if(map != null && map.containsKey(mapping.getKey())){ // && map.containsKey(relationalKey) && map.get(mapping.getKey()) != null && !map.get(mapping.getKey()).toString().isEmpty()){
				
				Map<String, String> valMap = options.containsKey("VALUEMAP") ? makeKVPair(options.get("VALUEMAP"),"\\|","="): new HashMap<String, String>();

				try {
					
					List<String> values = new ArrayList<String>();
					
					String str = map.get(mapping.getKey()).toString();
					String[] varr = new String[1];
					
					if(str.startsWith("[") && str.endsWith("]") && !str.equals("[]")){
					
						varr = str.substring(2,str.length() - 2).split(",");
						
					} else if(!str.equals("[]")) {
		
						varr[0] = str;
						
					}
					for(String value: varr){
						if(value != null){
							
					
							if(options.containsKey("REGEXEDIT")){
								value = value.replaceAll(options.get("REGEXEDIT"), "");
								
							}
							
							if(valMap.containsKey(value)){
								
								value = valMap.get(value);
								
							}
							
							if(entity instanceof ObservationFact){
								
								ObservationFact of = new ObservationFact("ObservationFact");
								
								of.setPatientNum(map.get(relationalKey).toString());
								
								of.setEncounterNum(map.get(relationalKey) + ":" + mapping.getKey());
								
								of.setInstanceNum(map.get(relationalKey) + ":" + mapping.getKey() + ":" + value);

								of.setConceptCd(mapping.getKey() + ':' + value);
								
								of.setValtypeCd(DEFAULT_VALUETYPE);
														
								of.setTvalChar(value);
								
								of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
								
								ents.add(of);
														
							} else if(entity instanceof ConceptDimension){
						
								ConceptDimension cd = new ConceptDimension("ConceptDimension");
														
								cd.setConceptCd(mapping.getKey() + ':' + value);
								
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
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		return ents;
	}
	/**
	 * Method for CSV Job see main comment above
	 * 
	 * Expecting a single 1 to 1 relationship
	 * @throws Exception 
	 */
	@Deprecated
	@Override
	public Set<Entity> generateTables(String[] data,
			CsvToI2b2TMMapping mapping, List<Entity> entities) throws Exception {
		// will be returned
		Set<Entity> ents = new HashSet<Entity>();
		
		for(Entity entity:entities){

			String col = mapping.getColumnNumber();
			
			String dataCell = new String();
			
			if((data.length) >= new Integer(col)){
				
				dataCell = data[new Integer(col) - 1];
				
			} else {
				//data length too short for specified col
				//System.err.println("logger");
				
			}
			// if there is no data skip
			if(dataCell != null && !dataCell.isEmpty()){
				if(entity instanceof ObservationFact){
					
					String encounterNum = mapping.getVisitIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getVisitIdColumn()) -1];
					String patientNum = mapping.getPatientIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getPatientIdColumn()) -1];
					
					String conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel() + ":" + dataCell;
					
					String providerId = mapping.getProviderIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getProviderIdColumn()) -1];
					String startDate = null;
					String modifierCd = null;
					String valtypeCd = mapping.getDataType();
	
					String tvalChar = dataCell;
					String nvalNum = null;
	
					String valueFlagCd = null;
					String quantityNum = null;
					String unitsCd = null;
					String endDate = null;
					String locationCd = null;
					String confidenceNum = null;
					String updateDate = null;
					String downloadDate = null;
					String importDate = null;
					String sourceSystemCd = this.DEFAULT_SOURCESYSTEM_CD;
					String uploadId = null;
					String observationBlob = null;
					String instanceNum = null;
					
					// Create of
					ObservationFact of = new ObservationFact("ObservationFact", encounterNum, patientNum, conceptCd, providerId, startDate, 
							modifierCd, valtypeCd, tvalChar, nvalNum, valueFlagCd, quantityNum, unitsCd, 
							endDate, locationCd, confidenceNum, updateDate, downloadDate, importDate, sourceSystemCd, uploadId, observationBlob, instanceNum);
					
					if(of.isValid()) ents.add(of);
					
				} if(entity instanceof ConceptDimension){
					
					List<String> path = Arrays.asList(this.ROOT_NODE, mapping.getCategoryCode(), mapping.getDataLabel(), dataCell);
					
					String conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel() + ":" + dataCell;
					
					String conceptPath = Entity.buildConceptPath(path);
	
					String nameChar = dataCell;
					
					String conceptBlob = null;
					
					String updateDate = null;
					
					String downloadDate = null;
					
					String importDate = null;
					
					String sourceSystemCd = this.DEFAULT_SOURCESYSTEM_CD;
					
					String uploadId = null;
					
					String tableName = null;	
					
					ConceptDimension cd = new ConceptDimension("ConceptDimension", conceptCd, conceptPath, nameChar, conceptBlob, 
							updateDate, downloadDate, importDate, sourceSystemCd, uploadId, tableName);
					
					if(cd.isValid()) ents.add(cd);
					
				} if(entity instanceof I2B2){
	
					List<String> path = Arrays.asList(this.ROOT_NODE, mapping.getCategoryCode(), mapping.getDataLabel(), dataCell);
		
					String cHlevel = Entity.calculateHlevel(Entity.buildConceptPath(path)).toString();
					String cFullName = Entity.buildConceptPath(path);
					String cName = dataCell;
					String cVisualAttributes = "LA";
					String cMetaDataXML = null;
					String cFactTableColumn = "CONCEPT_CD";
					String cTableName = "CONCEPT_DIMENSION";
					String cColumnName = "CONCEPT_PATH";
					String cOperator = "LIKE";
					String cDimCode = Entity.buildConceptPath(path);
					String cToolTip = Entity.buildConceptPath(path);
					String mAppliedPath = null;
					String mExclusionCd = null;
					String cColumnDataType = "T";
					String cSynonymCd = null;
					String cTotalNum = null;
					String cBaseCode = null;
					String cComment = null;
					String updateDate = null;
					String downloadDate = null;
					String importDate = null;
					String sourceSystemCd = this.DEFAULT_SOURCESYSTEM_CD;
					String valueTypeCd = "T";
					String i2b2Id = null;
					String cPath = null;
					String cSymbol = null;
					
					I2B2 i2b2 = new I2B2("I2B2", cHlevel, cFullName, cName, cSynonymCd, cVisualAttributes, cTotalNum, 
							cBaseCode, cMetaDataXML, cFactTableColumn, cTableName, cColumnName, cColumnDataType, cOperator, cDimCode, cComment, cToolTip, 
							updateDate, downloadDate, importDate, sourceSystemCd, valueTypeCd, i2b2Id, mAppliedPath, mExclusionCd, cPath, cSymbol);
					
					if(i2b2.isValid()) ents.add(i2b2);
				}
			} else {
				// no data found in <file> row <blah>
				//System.err.println("logger");
				
			}
			
		}
		
		return ents;
	}
	@Override
	public Set<Entity> generateTables(Mapping mapping, List<Entity> entities, List<Object> values,
			List<Object> relationalValues) throws Exception {
		
		if(mapping == null || entities == null || entities.isEmpty() || values == null || values.isEmpty()) {
			
			return new HashSet<Entity>();
			
		}
		
		Set<Entity> ents = new HashSet<Entity>();
		
		Map<String, String> options = mapping.buildOptions(mapping);
		
		Map<String, String> valMap = options.containsKey("VALUEMAP") ? makeKVPair(options.get("VALUEMAP"),"\\|","="): new HashMap<String, String>();
		
		for(Entity entity: entities){
			
			for(Object relationalValue: relationalValues) {
				for(Object v: values) {
					if( v == null || v instanceof Map ) {
						continue;
					}
					
					v = (v != null ) ? v.toString().replaceAll("[*|\\\\\\/<\\?%>\":]", ""): null;			
					
					String value = v.toString();
					if(value != null && !value.isEmpty()){						
				
						if(options.containsKey("REGEXEDIT") && value instanceof String){
							value = value.replaceAll(options.get("REGEXEDIT"), "");
							
						}
						
						if(valMap.containsKey(value)){
							
							value = valMap.get(value);
							
						}
						
						if(entity instanceof ObservationFact){
							
							ObservationFact of = new ObservationFact("ObservationFact");
							
							of.setPatientNum(relationalValue.toString());
							
							of.setEncounterNum(relationalValue + ":" + mapping.getKey());
														
							of.setInstanceNum(relationalValue + ":" + mapping.getKey() + ":" + Instant.now().toEpochMilli() );
							
							of.setConceptCd(mapping.getKey() + ":" + value);
							
							of.setValtypeCd(DEFAULT_VALUETYPE);

							of.setTvalChar(value);
							
							of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
							
							ents.add(of);
													
						} else if(entity instanceof ConceptDimension){
					
							ConceptDimension cd = new ConceptDimension("ConceptDimension");
													
							cd.setConceptCd(mapping.getKey() + ':' + value);
							
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
			}
		}
		return ents;
	}

	

	
}
