package etl.data.datatype.i2b2;

import java.util.ArrayList;
import java.util.Arrays;
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

public class Numeric extends DataType{

	private static String DEFAULT_VALUETYPE = "N";
			
	public Numeric(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<Entity> generateTables(Map map, Mapping mapping, List<Entity> entities,
			String relationalKey, String omissionKey) {
		
		Set<Entity> ents = new HashSet<Entity>();
		
		for(Entity entity: entities){			
											
				if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey)){
					try {
				
						if(entity instanceof ObservationFact){
														
							ObservationFact of = new ObservationFact("ObservationFact");
														
							of.setPatientNum(map.get(relationalKey).toString());
							
							of.setEncounterNum(map.get(relationalKey) + mapping.getKey());
							
							of.setConceptCd(mapping.getKey());
							
							of.setValtypeCd("N");
							
							of.setValueFlagCd("@");
							
							of.setNvalNum(map.get(mapping.getKey()).toString());
							
							of.setTvalChar("E");
							
							of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

							if(of.isValid()) ents.add(of);
								
	
						} else if(entity instanceof ConceptDimension){
					
							ConceptDimension cd =  new ConceptDimension("ConceptDimension");
							
							cd.setConceptCd(mapping.getKey());
							
							List<String> pathList = new ArrayList<>(Arrays.asList( Entity.ROOT_NODE, mapping.getRootNode(), mapping.getSupPath()));
							
							cd.setConceptPath(Entity.buildConceptPath(pathList));
							
							cd.setNameChar(mapping.getSupPath().toString());
							
							cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
							
							if(cd.isValid()) ents.add(cd);
							
						} else if(entity instanceof I2B2){
							
							I2B2 i2b2 = new I2B2("I2B2");
							
							List<String> pathList = new ArrayList<>(Arrays.asList( Entity.ROOT_NODE, mapping.getRootNode(), mapping.getSupPath()));
							
							i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList)).toString());
							
							i2b2.setcFullName(Entity.buildConceptPath(pathList));
							
							i2b2.setcName(mapping.getSupPath().toString());
							
							i2b2.setcBaseCode(mapping.getKey());
							
							i2b2.setcVisualAttributes("LA");
							
							i2b2.setcFactTableColumn("CONCEPT_CD");
							
							i2b2.setcTableName("CONCEPT_DIMENSION");
							
							i2b2.setcColumnName("CONCEPT_PATH");
							
							i2b2.setcColumnDataType("T");
							
							i2b2.setcOperator("LIKE");
							
							i2b2.setcMetaDataXML(C_METADATAXML);
							
							i2b2.setcDimCode(Entity.buildConceptPath(pathList));
							
							i2b2.setcToolTip(Entity.buildConceptPath(pathList));
							
							i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
							
							i2b2.setmAppliedPath("@");
							
							if(i2b2.isValid()) ents.add(i2b2);
							
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
	@Override
	public Set<Entity> generateTables(String[] data,
			CsvToI2b2TMMapping mapping, List<Entity> entities) throws Exception {
		
		Set<Entity> ents = new HashSet<Entity>();
		
		for(Entity entity:entities){
			
			String col = mapping.getColumnNumber();
			
			String dataCell = new String();
			
			if(data.length >= new Integer(col)){
				
				dataCell = data[new Integer(col) - 1];
				
			} else {
				// logger
				
			}
				if(dataCell != null && !dataCell.isEmpty()){			
				if(entity instanceof ObservationFact){
					
					String encounterNum = mapping.getVisitIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getVisitIdColumn()) -1];
					String patientNum = mapping.getPatientIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getPatientIdColumn()) -1];
					
					String conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel();
					
					String providerId = mapping.getProviderIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getProviderIdColumn()) -1];
					String startDate = null;
					String modifierCd = null;
					String valtypeCd = mapping.getDataType();
					
					String tvalChar = "E";
					String nvalNum = dataCell;
					
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
					
					List<String> path = Arrays.asList(this.ROOT_NODE, mapping.getCategoryCode(), mapping.getDataLabel());
	
					String conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel();
					
					String conceptPath = Entity.buildConceptPath(path);
	
					String nameChar = mapping.getDataLabel();;
							
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
	
					List<String> path = Arrays.asList(this.ROOT_NODE, mapping.getCategoryCode(), mapping.getDataLabel());
					
					String cHlevel = Entity.calculateHlevel(Entity.buildConceptPath(path)).toString();
	
					String cFullName = Entity.buildConceptPath(path);
					String cName = mapping.getDataLabel();
					String cVisualAttributes = "LA";
					String cMetaDataXML = "<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>";
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
				// no data found in <file> row <blah> System.err.println("logger");

				
			}
				
		}
		
		return ents;
	}


}
