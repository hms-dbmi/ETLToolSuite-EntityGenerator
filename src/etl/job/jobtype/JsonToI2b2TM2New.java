package etl.job.jobtype;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.csvreader.CsvReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import etl.data.datasource.JSONDataSource;
import etl.data.datasource.JSONDataSource2;
import etl.data.datatype.DataType;
import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.Export;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptCounts;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObjectMapping;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.data.export.entities.i2b2.PatientDimension;
import etl.data.export.entities.i2b2.PatientMapping;
import etl.data.export.entities.i2b2.PatientTrial;
import etl.data.export.entities.i2b2.utils.ColumnSequencer;
import etl.data.export.i2b2.ExportI2B2;
import etl.job.jobtype.properties.JobProperties;
import etl.job.jsontoi2b2tm.entity.Mapping;
import etl.job.jsontoi2b2tm.entity.PatientMapping2;

import static java.nio.file.StandardOpenOption.*;

public class JsonToI2b2TM2New extends JobType {
	private static final Logger logger = LogManager.getLogger(JsonToI2b2TM2New.class);
	
	//required configs
	private static String FILE_NAME; 
	
	private static String WRITE_DESTINATION; 
			
	private static String MAPPING_FILE; 
	
	private static String PATIENT_MAPPING_FILE;
	// optional
	private static char MAPPING_DELIMITER;
	
	private static String RELATIONAL_KEY = "fields:uuid";
			
	private static Map<String,List<String>> OMISSIONS_MAP = new HashMap<String, List<String>>();
	{
		
		OMISSIONS_MAP.put(":fields:activestatus", Arrays.asList("A"));
		OMISSIONS_MAP.put(":fields:encoded_relation", Arrays.asList("null","",null));
	};
	
	/// Internal Config
	List<ColumnSequencer> sequencers = new ArrayList<ColumnSequencer>(); 	
	
	private static boolean LEVEL_1_ACCESS = false;
	
	private static String FILE_TYPE = "JSONFILE";
	
	private static OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, TRUNCATE_EXISTING };
		
	private static final String ARRAY_FORMAT = "JSONFILE";

	private static final String OUTPUT_FILE_EXTENSION = ".csv";

	private static final boolean MAPPING_SKIP_HEADER = true;

	private static final char MAPPING_QUOTED_STRING = '"';

	private static Class DATASOURCE_FORMAT = null;
	private static String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";

	private static List<String> EXPORT_TABLES = 
			new ArrayList<String>(Arrays.asList("ModifierDimension", "ObservationFact", "I2B2", 
					"ConceptDimension"));
	
	int inc = 100;
	
	int x = 0;
	
	int maxSize;

	public static List<Entity> entities;
	
	public JsonToI2b2TM2New(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	/**
	 * This is a temporary fix to solve the dummy row issue needed to ensure Trials works 
	 * All of this should be removed once bug is fixed in tm
	 * @throws Exception 
	 * 
	 */
	
	private static void performRequiredTempFixes(Set<Entity> entities) throws Exception {
		entities.add(buildDummyForTrial());
		forceSourceSystemCds(entities);

		buildL1Security(entities);


		//Set<Entity> cleaned = cleanChars(entities);
		
		//entities = new HashSet<Entity>();
		
		//entities.addAll(cleaned);
		
	}
	
	private static void forceSourceSystemCds(Set<Entity> entities) throws IllegalArgumentException, IllegalAccessException {
		for(Entity entity: entities) {
			List<Field> fields = Arrays.asList(entity.getClass().getDeclaredFields());
			boolean hasField = false;
			for(Field field: fields) {
				// if class contains sourcesystemcd force new value
				if(field.getName().equalsIgnoreCase("sourcesystemcd")) {
					if(entity instanceof PatientDimension) {
						String patientNum = ((PatientDimension) entity).getPatientNum();
						((PatientDimension) entity).setSourceSystemCD(Entity.SOURCESYSTEM_CD + ":" + patientNum);
					} else if(entity instanceof I2B2) {
						((I2B2) entity).setcComment(Entity.SOURCESYSTEM_CD);;
						((I2B2) entity).setSourceSystemCd(Entity.SOURCESYSTEM_CD);
					} else {
						field.setAccessible(true);
						field.set(entity, Entity.SOURCESYSTEM_CD);
					}
				}
			}
		}
		
	}

	private static void buildL1Security(Set<Entity> entities) throws Exception {
		Set<Entity> newEnts = new HashSet<Entity>();
		if(LEVEL_1_ACCESS) {
			for(Entity entity: entities) {
				if(entity instanceof PatientDimension) {
					ObservationFact of = new ObservationFact("ObservationFact");
					of.setPatientNum(((PatientDimension)entity).getPatientNum());
					of.setEncounterNum("-1");
					of.setConceptCd("SECURITY");
					of.setProviderId("@");
					of.setModifierCd(Entity.SOURCESYSTEM_CD);
					of.setValtypeCd("T");
					of.setTvalChar("EXP:PUBLIC");
					of.setValueFlagCd("@");
					of.setLocationCd("@");
					newEnts.add(of);
				}
			}
		}
		entities.addAll(newEnts);
	}
	private static I2B2 buildDummyForTrial() throws Exception{
		I2B2 i2b2 = new I2B2("I2B2");
		
		i2b2.setcHlevel("0");
		i2b2.setcFullName("\\a\\");
		i2b2.setcName("a");
		i2b2.setcSynonymCd("N");
		i2b2.setcVisualAttributes("CA");
		i2b2.setcTotalNum("");
		i2b2.setcBaseCode("");
		i2b2.setcMetaDataXML("");
		i2b2.setcFactTableColumn("CONCEPT_CD");
		i2b2.setcTableName("CONCEPT_DIMENSION");
		i2b2.setcColumnName("CONCEPT_PATH");
		i2b2.setcColumnDataType("T");
		i2b2.setcOperator("LIKE");
		i2b2.setcDimCode(i2b2.getcFullName());
		i2b2.setcComment("trial:" + Entity.SOURCESYSTEM_CD);
		i2b2.setcToolTip(i2b2.getcFullName());
		i2b2.setmAppliedPath("");
		i2b2.setUpdateDate("");
		i2b2.setDownloadDate("");;
		i2b2.setImportDate("");
		i2b2.setSourceSystemCd(Entity.SOURCESYSTEM_CD);
		i2b2.setValueTypeCd("");
		i2b2.setmExclusionCd("");
		i2b2.setcPath("");
		i2b2.setcSymbol("");
		
		return i2b2;
	}
	
	/** primary process of this job.
	 * 
	 * @see etl.job.jobtype.JobType#runJob()
	 * 
	 * Loads a datafile and processes it based on the mapping file.
	 */
	@SuppressWarnings("static-access")
	@Override
	public void runJob(JobProperties jobProperties) {
		// set global variables from passed job properties file
		// set type of array format used for object arrays 
		// this will have to be changed to be more dynamic
		//Objectarray.ARRAY_FORMAT = this.ARRAY_FORMAT;
		
		// Entity object that will hold all the wanted entities to be generated
		Set<Entity> builtEnts = new HashSet<Entity>();
		
		try {	
			setVariables(jobProperties);

			// needs to handle multiple files 
			File dataFile = new File(FILE_NAME);
			
			List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

			List<PatientMapping2> patientMappingFile = PatientMapping2.class.newInstance().generateMappingList(PATIENT_MAPPING_FILE, MAPPING_DELIMITER);
			
			if(dataFile.exists()){
				// Read datafile into a List of LinkedHashMaps.
				// List should be objects that will be used for up and down casting through out the job process.
				// Using casting will allow the application to be very dynamic while also being type safe.
				
				List list = buildRecordList();

				logger.info("generating tables");
								
				for(Object o: list){
				
					if( o instanceof LinkedHashMap ) {
						LinkedHashMap record = ( LinkedHashMap ) o;
						
						builtEnts.addAll(processEntities(mappingFile, record));	
						
						for(Entity entity: processPatientEntities(patientMappingFile,record)) {
							if(entity instanceof PatientDimension) {
								if(((PatientDimension) entity).isValid()) {
							
									builtEnts.addAll(processPatientEntities(patientMappingFile,record));
								
								}
							}
						}

					}
				}
				
				list = null;
				logger.info("Filling in Tree");
				builtEnts.addAll(thisFillTree(builtEnts));
								
				logger.info("Generating ConceptCounts");
				builtEnts.addAll(ConceptCounts.generateCounts2(builtEnts));
				logger.info("finished generating tables");

			} else {
				logger.error("File " + dataFile + " Does Not Exist!");
			}

			logger.info("Generating sequences");
			

			Set<ObjectMapping> oms = new HashSet<ObjectMapping>();
			
			for(ColumnSequencer seq: sequencers ) {
				
				oms.addAll(seq.generateSeqeunce(builtEnts));
				
			}
			Set<PatientMapping> pms = PatientMapping.objectMappingToPatientMapping(oms);
			
			builtEnts.addAll(pms);
			builtEnts.addAll(oms);


			logger.info("Performing temp fixes");
			//perform any temp fixes in method called here
			performRequiredTempFixes(builtEnts);		
		} catch (Exception e) {
			e.printStackTrace();
			logger.catching(Level.ERROR,e);
			
		} 
		
		try {
			Map<Path, List<String>> paths = Export.buildFilestoWrite(builtEnts, WRITE_DESTINATION, OUTPUT_FILE_EXTENSION);
			
			Export.writeToFile(paths, WRITE_OPTIONS);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		/*
		for(Entity entity: builtEnts){

			try {
				if(entity != null) {
					Export.writeToFile(WRITE_DESTINATION + entity.getClass().getSimpleName() + ".csv", entity.toCsv(), WRITE_OPTIONS);
				}
			} catch (IOException e) {
				System.out.println(e);
				// TODO Auto-generated catch block
				logger.catching(e);
				
			}
						
		}
		*/
	}
	


	private boolean iterateOmkeys(List<String> omkeys, Map map) {
		
		for(String omkey:omkeys) {
			
			if(map.containsKey(omkey)) {
				
				if(map.get(omkey) instanceof Map) {
				
					omkeys.remove(omkeys.indexOf(omkey));
					
					iterateOmkeys(omkeys, (Map) map.get(omkey));
				
				} else {
					
					if(map.get(omkey) == null){
						
						return true;
						
					}
				}
			} else {
				
				return false;
				
			}
		}
		
		return false;
		
	}
	
	private boolean isOmitted(Map record) {
		boolean isOmitted = false;
		if(!OMISSIONS_MAP.isEmpty()) {
			if(record != null || !record.isEmpty()) {
				for(String omkeyfull: OMISSIONS_MAP.keySet()) {
					
					ArrayList<String> omkeys = new ArrayList(Arrays.asList(omkeyfull.split(":")));
					omkeys.remove(0);
					isOmitted = isOmitted(omkeys, record, omkeyfull);
					if(isOmitted == true) break;
				}		
			
			}
		} 
		return isOmitted;
	}
	
	private boolean isOmitted(ArrayList<String> omkeys, Object record, String currFullKey) {
		
		if(record instanceof Map) {
			boolean isOmitted = false;
			Map<String, Object> rec = (LinkedHashMap<String,Object>) record;
			
			if(rec.keySet().contains(omkeys.get(0))) {
				Object r = rec.get(omkeys.get(0));
				
				if(r instanceof Map) {
					omkeys.remove(0);
					isOmitted = isOmitted(omkeys, r, currFullKey);
				} else {
					isOmitted = isOmitted(omkeys, r, currFullKey);
				}
			} else {
				isOmitted = true;
			}
			return isOmitted;
			
		} else if(record instanceof List) {
			List recs = (List) record;
			boolean isOmitted = false;

			for(Object r: recs) {
				isOmitted = isOmitted(omkeys, r, currFullKey);
				if(isOmitted == true) break;
			}
			return isOmitted;

		} else if(record instanceof String) {
			boolean isOmitted = false;

			List<String> l = OMISSIONS_MAP.get(currFullKey);
			if(l.contains(record.toString())) {
				isOmitted = false;
			} else {
				isOmitted = true;
			}
			
			return isOmitted;

		} else if(record == null) {
			boolean isOmitted = false;

			List<String> l = OMISSIONS_MAP.get(currFullKey);
			
			if(l.contains(null)) {
				isOmitted = false;
			} else {
				isOmitted = true;
			}
			return isOmitted;

		} else {
			return true;
		}
	}
	
	private List<Object> findValueByKey2(Object record, ArrayList<String> keys) throws Exception {
			
		List<Object> list = new ArrayList<Object>();
		
		
		if( !keys.isEmpty() && keys.size() > 0 ) {
			
			String k = keys.get(0);
			// presume it must be a map
			if(record instanceof Map) {
				keys.remove(0);
				
				Map<String,Object> record2 = (LinkedHashMap) record;
				
				if(record2.containsKey(k)) {
					
					Object o = record2.get(k);

					list = findValueByKey2(o, keys);
				}
			} else if ( record instanceof List) {
				List recs = (ArrayList<Object>) record;

				for(Object o: recs) {
					
					list = findValueByKey2(o, keys);
					
				}
				
				
			} 
		} else if( record instanceof Collection){
			list.addAll((Collection<? extends Object>) record);
		} else {
			list.add(record);
		}
		return list;
		
	}
	

	@SuppressWarnings("unchecked")
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
						if(!iter.hasNext()) {

							if(o instanceof Map) {
								
								record2 = new LinkedHashMap((LinkedHashMap) o);
								
								l.add(record2);
							
							}
						} else {
							return (ArrayList) obj;
							
						}
						
					}

					return l;
						
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
	
	// OMITS Records if omission key is given. 
	private List<Entity> processEntities(List<Mapping> mappings, Map record) throws Exception{

		List<Entity> builtEnts = new ArrayList<Entity>();
		
		boolean isOmitted = isOmitted(record);
		
		@SuppressWarnings("unused")
		List<Object> relationalValue = findValueByKey(record,RELATIONAL_KEY.split(":"));
		/*
		for(String omkeyfull: OMISSIONS_MAP.keySet()) {
			
			String[] omkeys = omkeyfull.split(":");
			
			isOmitted = iterateOmkeys(Arrays.asList(omkeys), record);
			
		}
*/		
		
		if(!isOmitted){
			
			for(Mapping mapping: mappings){
				
				DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));
				
				if(!mapping.getDataType().equalsIgnoreCase("OMIT")){
					
					ArrayList<String> key = new ArrayList(Arrays.asList(mapping.getKey().split(":")));
					if(key.get(0).isEmpty())key.remove(0);
					
					List<Object> values = findValueByKey2(record, key);
					Map<String,String> options = mapping.buildOptions(mapping);
					
					if(!options.isEmpty()) {
						if(options.containsKey("TYPE")) {
							String type = options.get("TYPE");
							if(type.equalsIgnoreCase("datediffin")) {

								String dateDiffFrom = options.containsKey("DIFFFROM") ? options.get("DIFFFROM").replaceAll("-",":"): null;
								
								String mask = options.containsKey("MASK") ? options.get("MASK"): null;

								String diffIn = options.containsKey("DIFFIN") ? options.get("DIFFIN"): null;
								
								LocalDate startDateInclusive = null;
								
								LocalDate endDateExclusive = null;
								
								if(dateDiffFrom.equalsIgnoreCase("sysdate")) {
									startDateInclusive = LocalDate.now();
								} else {
									
									Object fromO = 
											findValueByKey(new LinkedHashMap(record), dateDiffFrom.split(":")).get(0);

									if(mask != null) {
										if(fromO != null) {
											
											String from = fromO.toString();
											
											startDateInclusive = LocalDate.parse(from, DateTimeFormatter.ofPattern(mask) );
											
										}
										
									} else throw new Exception("No mask given");							
								}
								for(int x = 0; x < values.size(); x++) {
									
									String value = values.get(0).toString();
									
									endDateExclusive = value.isEmpty() ? null: LocalDate.parse(value, DateTimeFormatter.ofPattern(mask) );
									
									if(endDateExclusive != null) {
										
										Period period = Period.between(endDateExclusive, startDateInclusive );

										values.remove(x);
										
										if(diffIn.equalsIgnoreCase("years")) {
											values.add(x, period.getYears());
										}
									}								
								}
							} else if(type.equalsIgnoreCase("datedifffrom")) {
								String dateDiffFrom = options.containsKey("DIFFFROM") ? options.get("DIFFFROM").replaceAll("-",":"): null;
								
								String mask = options.containsKey("MASK") ? options.get("MASK"): null;

								String diffIn = options.containsKey("DIFFIN") ? options.get("DIFFIN"): null;
								
								LocalDate startDateInclusive = null;
								
								LocalDate endDateExclusive = null;
								
								if(dateDiffFrom.equalsIgnoreCase("sysdate")) {
									startDateInclusive = LocalDate.now();
								} else {
									
									Object fromO = 
											findValueByKey(new LinkedHashMap(record), dateDiffFrom.split(":")).get(0);

									if(mask != null) {
										if(fromO != null) {
											
											String from = fromO.toString();
											
											startDateInclusive = LocalDate.parse(from, DateTimeFormatter.ofPattern(mask) );
											
										}
										
									} else throw new Exception("No mask given");							
								}
								for(int x = 0; x < values.size(); x++) {
									if(values.get(0) != null) {
										String value = values.get(0).toString();
										
										endDateExclusive = value.isEmpty() ? null: LocalDate.parse(value, DateTimeFormatter.ofPattern(mask) );
										
										if(endDateExclusive != null) {
											
											Period period = Period.between(startDateInclusive, endDateExclusive);
											
											values.remove(x);
											if(diffIn.equalsIgnoreCase("years")) {
												values.add(x, period.getYears());
											}
										}					
									}
								}							
							}
						}
					}
					Mapping m2 = mapping;
					
					if(m2.getKey().toCharArray()[0] == ':') m2.setKey(m2.getKey().replaceFirst(":", ""));
					
					Set<Entity> newEnts = dt.generateTables(m2, entities, values, relationalValue);
					
					if(newEnts != null && newEnts.size() > 0){
					
						builtEnts.addAll(newEnts);
					
					}
					
				}
						
			}

		}
		return builtEnts;		
	}

	private List<Entity> processPatientEntities(List<PatientMapping2> mappings,
			LinkedHashMap record) throws Exception {
		
		List<Entity> entities = new ArrayList<Entity>();
		boolean isOmitted = isOmitted(record);

		PatientDimension pd = new PatientDimension("PatientDimension");
		
		PatientTrial pt = new PatientTrial("PatientTrial");
		
		if(!isOmitted){
			Map<String,List<Object>> valueMap = new HashMap<String,List<Object>>();
			
			for(PatientMapping2 mapping: mappings){
				ArrayList<String> patKeys = new ArrayList(Arrays.asList(mapping.getPatientKey().split(":")));
				patKeys.remove(0);
				List<Object> values = (ArrayList<Object>) findValueByKey2(new LinkedHashMap(record), patKeys);
				
				Map<String, String> options = mapping.buildOptions(mapping);
				
				if(!options.isEmpty() && !values.isEmpty() ) {
					if(options.containsKey("TYPE")) {
						
						String type = options.get("TYPE");
						
						if(type.equalsIgnoreCase("datedifffrom")) {
							String dateDiffFrom = options.containsKey("DIFFFROM") ? options.get("DIFFFROM").replaceAll("-",":"): null;
							
							String mask = options.containsKey("MASK") ? options.get("MASK"): null;

							String diffIn = options.containsKey("DIFFIN") ? options.get("DIFFIN"): null;
							
							LocalDate startDateInclusive = null;
							
							LocalDate endDateExclusive = null;
							
							if(dateDiffFrom.equalsIgnoreCase("sysdate")) {
								startDateInclusive = LocalDate.now();
							} else {
								
								Object fromO = 
										findValueByKey(new LinkedHashMap(record), dateDiffFrom.split(":")).get(0);

								if(mask != null) {
									if(fromO != null) {
										
										String from = fromO.toString();
										
										startDateInclusive = LocalDate.parse(from, DateTimeFormatter.ofPattern(mask) );
										
									}
									
								} else throw new Exception("No mask given");							
							}
							for(int x = 0; x < values.size(); x++) {
								
								if(values.get(x) == null){
									break;
								}
								
								String value = values.get(0).toString();
								
								endDateExclusive = value.isEmpty() ? null: LocalDate.parse(value, DateTimeFormatter.ofPattern(mask) );
								
								if(endDateExclusive != null) {
									
									Period period = Period.between(startDateInclusive, endDateExclusive);
									
									values.remove(x);
									if(diffIn.equalsIgnoreCase("years")) {
										values.add(x, period.getYears());
									}
								}								
							}							
						} 
					} if(options.containsKey("VALUEMAP")) {
						
						Map<String, String> valMap = options.containsKey("VALUEMAP") ? makeKVPair(options.get("VALUEMAP"),"\\|","="): new HashMap<String, String>();
						
						for(int x = 0; x < values.size(); x++) {

							Object value = values.get(x);
							
							if(valMap.containsKey(value)){
								
								value = valMap.get(value);
								
								values.remove(x);
								
								values.add(x, value);
								
							}
							
						}
													
					}
				}
					
				valueMap.put(mapping.getPatientColumn(), values);
				
			}
			pd = new PatientDimension("PatientDimension",record,valueMap);
			pt = new PatientTrial("PatientTrial", record, valueMap);
			
			
			entities.add(pd);
			entities.add(pt);
		}
		return entities;			
	}

	private void processRecord(ArrayList values, String currKey, Mapping mapping, List<Entity> entities) {
		List<Entity> builtEnts = new ArrayList<Entity>();
		
		try {
			
			DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));
			if(!mapping.getDataType().equalsIgnoreCase("OMIT")){
				
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		}				
	}

	private void processEntitesMap(LinkedHashMap obj, String currKey, List<Entity> entities, String rELATIONAL_KEY2) {
		// TODO Auto-generated method stub
		
	}

	private List<Entity> buildEntities() throws InstantiationException, IllegalAccessException {
		
		return Entity.buildEntityList(ENTITY_PACKAGE, EXPORT_TABLES);
		
	}

	private List buildRecordList() throws Exception{
		
		List nodes = JSONDataSource2.buildObjectMap(new File(FILE_NAME), DATASOURCE_FORMAT);
	
		
		return nodes;
	}	
	
	@SuppressWarnings("unused")
	private Map<String,Map<String,String>> buildMap() throws Exception{
		
		JSONDataSource jds = new JSONDataSource(FILE_TYPE);
		
		JsonNode nodes = jds.buildObjectMap(new File(FILE_NAME));
		
		Map<String, Map<String,String>> parent = new HashMap<String, Map<String,String>>();
		
		Map<String,String> child = new HashMap<String, String>();
		
		Integer x = 0;
		
		for(JsonNode node: nodes){
			
			child = jds.processNodeToMap(node);
		
			parent.put(x.toString(), child);
				
			x++;
									
		}
		
		return parent;
		
	}

	@Override
	public void setVariables(JobProperties jobProperties) throws ClassNotFoundException {
		
		//required
		FILE_NAME = jobProperties.getProperty("filename"); ; 
		
		WRITE_DESTINATION = jobProperties.getProperty("writedestination"); 
				
		MAPPING_FILE = jobProperties.getProperty("mappingfile"); 
		
		PATIENT_MAPPING_FILE = jobProperties.getProperty("patientmappingfile");
				
		//optional
		MAPPING_DELIMITER = jobProperties.containsKey("mappingdelimiter") && jobProperties.getProperty("mappingdelimiter").toCharArray().length == 1 ? 
				jobProperties.getProperty("mappingdelimiter").toCharArray()[0] : ',';
		
		RELATIONAL_KEY = jobProperties.containsKey("relationalkey") ? 
				jobProperties.getProperty("relationalkey"): RELATIONAL_KEY;
		
		Entity.SOURCESYSTEM_CD = jobProperties.getProperty("sourcesystemcd");
		System.out.println(Entity.SOURCESYSTEM_CD);
		
		DATASOURCE_FORMAT = jobProperties.containsKey("datasourceformat") ? Class.forName(jobProperties.getProperty("datasourceformat")): null;

		sequencers.add(new ColumnSequencer(Arrays.asList("ConceptDimension","ObservationFact"), "conceptCd", "CONCEPTCD", "I2B2", 1, 1));

		sequencers.add(new ColumnSequencer(Arrays.asList("ObservationFact"), "encounterNum", "ENCNUM", "I2B2", 1, 1));
		
		sequencers.add(new ColumnSequencer(Arrays.asList("PatientDimension","ObservationFact","PatientTrial"), "patientNum", "ID", "I2B2", 1, 1));
		
		ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
		
		if(jobProperties.containsKey("level1")) {
			LEVEL_1_ACCESS = jobProperties.get("level1").toString().equalsIgnoreCase("Y") ||
				jobProperties.get("level1").toString().equalsIgnoreCase("YES") ||
				jobProperties.get("level1").toString().equalsIgnoreCase("TRUE") ? true: false;
		}
		try {
			entities = buildEntities();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private Collection<? extends Entity> thisFillTree(Set<Entity> entities) throws Exception {
		List<I2B2> i2b2 = new ArrayList<I2B2>();
		// fill in tree
		if(EXPORT_TABLES.contains("I2B2")){
		
			for(Entity entity: entities){
				
				if(entity instanceof I2B2){
					i2b2.add((I2B2) entity);
					
				}
				
			}
			
		}
		return I2B2.fillTree(i2b2);
	}
}
