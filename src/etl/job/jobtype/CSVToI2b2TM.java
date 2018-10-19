package etl.job.jobtype;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.csvreader.CsvReader;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

import au.com.bytecode.opencsv.CSVReader;
import etl.data.datasource.CSVDataSource2;
import etl.data.datasource.JSONDataSource;
import etl.data.datasource.entities.json.udn.UDN;
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

public class CSVToI2b2TM extends JobType {
	private static boolean LEVEL_1_ACCESS = true;

	//required configs
	private static String FILE_NAME; 
	
	private static String WRITE_DESTINATION; 
			
	private static String MAPPING_FILE; 
	
	private static String PATIENT_MAPPING_FILE;
	// optional
	private static char MAPPING_DELIMITER;
	
	private static char MAPPING_QUOTED_STRING = 0;

	private static boolean MAPPING_SKIP_HEADER;

	private static boolean RELATIONAL_KEY_OVERRIDE = true;

	private static String RELATIONAL_KEY = "0";
	
	private static boolean DO_SEQUENCING = true;
	
	private static boolean DO_PATIENT_NUM_SEQUENCE = true;
	
	private static boolean DO_CONCEPT_CD_SEQUENCE = true;
	
	private static boolean DO_ENCOUNTER_NUM_SEQUENCE = true;
	
	private static boolean DO_INSTANCE_NUM_SEQUENCE = true;
	
	private static Map<String,List<String>> OMISSIONS_MAP = new HashMap<String, List<String>>();

	List<ColumnSequencer> sequencers = new ArrayList<ColumnSequencer>();
	
	public static Class DATASOURCE_FORMAT;
	/// Internal Config
	private static String FILE_TYPE = "JSONFILE";
	
	private static OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, APPEND };
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";
	
	private static final String ARRAY_FORMAT = "JSONFILE";

	private static final String OUTPUT_FILE_EXTENSION = ".csv";

	private static final boolean APPEND_FILES = false;

	private static Integer CONCEPT_CD_STARTING_SEQ = 1;

	private static Integer ENCOUNTER_NUM_STARTING_SEQ = 1;

	private static Integer PATIENT_NUM_STARTING_SEQ = 1;


	private static File DICT_FILE = null;
	
	private static List<String> EXPORT_TABLES = 
			new ArrayList<String>(Arrays.asList("ModifierDimension", "ObservationFact", "I2B2", 
					"ConceptDimension"));
	
	int inc = 100;
	
	int x = 0;
	
	int maxSize;

	private boolean IS_DIR;

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
		
		fixcBaseCode(entities);
		
	}
	
	private static void fixcBaseCode(Set<Entity> entities) {
		Map<String,String> map = new HashMap<String,String>();
		for(Entity entity: entities) {
			if( entity instanceof ConceptDimension) {
				map.put(((ConceptDimension) entity).getConceptPath(), ((ConceptDimension) entity).getConceptCd());
			}
		}
		for(Entity entity: entities) {
			if( entity instanceof I2B2) {
				if(map.keySet().contains(((I2B2) entity).getcFullName())) {
					((I2B2) entity).setcBaseCode(map.get(((I2B2) entity).getcFullName()));
				}
			}
		}
		
		
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
	
	public CSVToI2b2TM(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
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
		
		// Entity object that will hold all the wanted entities to be generated

		Set<Entity> builtEnts = new HashSet<Entity>();
		
		Set<String> patientIds = new HashSet<String>();
		
		try {	
			logger.info("Setting Variables");
			setVariables(jobProperties);
		
			File data = new File(FILE_NAME);
			
			logger.info("Reading Mapping files");
			
			List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

			List<PatientMapping2> patientMappingFile = 
					!PATIENT_MAPPING_FILE.isEmpty() ? PatientMapping2.class.newInstance().generateMappingList(PATIENT_MAPPING_FILE, MAPPING_DELIMITER): new ArrayList<PatientMapping2>();
			
					
			// set relational key if not set in config
			if(RELATIONAL_KEY_OVERRIDE == false) {
				for(PatientMapping2 pm: patientMappingFile) {
					if(pm.getPatientColumn().equalsIgnoreCase("PatientNum")) {
						RELATIONAL_KEY = pm.getPatientKey().split(":")[1];
						break;
					}
					
				}
			}
			Map<String,Map<String,String>> datadic = DICT_FILE.exists() ? generateDataDict(DICT_FILE): new HashMap<String,Map<String,String>>();
			
			logger.info("Finished Reading Mapping Files");

			if(data.exists()){
				// Read datafile into a List of LinkedHashMaps.
				// List should be objects that will be used for up and down casting through out the job process.
				// Using casting will allow the application to be very dynamic while also being type safe.
				
				try {
				
					logger.info("generating patients");
					
					Map<String, Map<String,String>> patientList = buildPatientRecordList(data,patientMappingFile, datadic);
					
					for(String key: patientList.keySet()) {
						Map<String,String> pat = patientList.get(key);
						
						if(pat.containsKey("patientNum")) {
	
							patientIds.add(pat.get("patientNum"));
			
						}
						builtEnts.addAll(processPatientEntities(patientList.get(key)));
						
					}
					
					logger.info(patientIds.size() + " Patients Generated.");
					
				} catch (Exception e) {
					logger.error("Error building patients");
					logger.error(e);
				}
				
				logger.info("generating tables");
				try {
					logger.info("Reading Data Files");

					List recs = buildRecordList(data, mappingFile, datadic);

					logger.info("Finished reading Data Files");

					logger.info("Building table Entities");

					for(Object o: recs){
						
						if( o instanceof LinkedHashMap ) {
							
							builtEnts.addAll(processEntities(mappingFile,( LinkedHashMap ) o));	
	
						}
					}
					
					logger.info("Finished Building Entities");
					
				} catch (Exception e) {
					logger.error("Error Processing data files");
					logger.error(e);
				}
				
				logger.info("Filling in Tree");
				builtEnts.addAll(thisFillTree(builtEnts));
								
				logger.info("Generating ConceptCounts");
				
				builtEnts.addAll(ConceptCounts.generateCounts2(builtEnts, patientIds));
				
				logger.info("finished generating tables");

			} else {
				logger.error("File " + data + " Does Not Exist!");
			}
			//builtEnts.addAll(thisFillTree(builtEnts));
			// for testint seqeunces move this to a global variable and generate it from properties once working;
			logger.info("Generating sequences");
			
			List<ColumnSequencer> sequencers = new ArrayList<ColumnSequencer>();
			
			if(DO_SEQUENCING) {
				
				if(DO_CONCEPT_CD_SEQUENCE) sequencers.add(new ColumnSequencer(Arrays.asList("ConceptDimension","ObservationFact"), "conceptCd", "CONCEPTCD", "I2B2", CONCEPT_CD_STARTING_SEQ, 1));
	
				if(DO_ENCOUNTER_NUM_SEQUENCE) sequencers.add(new ColumnSequencer(Arrays.asList("ObservationFact"), "encounterNum", "ENCOUNTER", "I2B2", ENCOUNTER_NUM_STARTING_SEQ, 1));
				
				if(DO_INSTANCE_NUM_SEQUENCE) sequencers.add(new ColumnSequencer(Arrays.asList("ObservationFact"), "instanceNum", "INSTANCE", "I2B2", ENCOUNTER_NUM_STARTING_SEQ, 1));
	
				if(DO_PATIENT_NUM_SEQUENCE) sequencers.add(new ColumnSequencer(Arrays.asList("PatientDimension","ObservationFact","PatientTrial"), "patientNum", "ID", "I2B2", PATIENT_NUM_STARTING_SEQ, 1));

			}
			
			//Set<ObjectMapping> oms = new HashSet<ObjectMapping>();
			logger.info("Applying sequences");
			Set<PatientMapping> pms = new HashSet<PatientMapping>();		
			
			for(ColumnSequencer seq: sequencers ) {
				logger.info("Performing sequence: " + seq.entityColumn + " for e ( " + seq.entityNames + " )" );
				
				pms.addAll(seq.generateSeqeunce2(builtEnts));

			}
			
			logger.info("Building Patient Mappings");
			//Set<PatientMapping> pms = PatientMapping.objectMappingToPatientMapping(oms);
			logger.info("Finished Building Patient Mappings");
			
			
			try {

				logger.info("Performing temp fixes");
				//perform any temp fixes in method called here
				performRequiredTempFixes(builtEnts);
				

				//Map<Path, List<String>> paths = Export.buildFilestoWrite(builtEnts, WRITE_DESTINATION, OUTPUT_FILE_EXTENSION);
				
				logger.info("writing tables");
				
				if(APPEND_FILES == false) {
					
					logger.info("Cleaning write directory - " + new File(WRITE_DESTINATION).getAbsolutePath());
					
					Export.cleanWriteDir(WRITE_DESTINATION);
					
				}
				//Export.writeToFile(paths, WRITE_OPTIONS);
				
				logger.info("Writing Entites to " + new File(WRITE_DESTINATION).getAbsolutePath());
				
				Export.writeToFile(builtEnts, WRITE_DESTINATION, OUTPUT_FILE_EXTENSION, WRITE_OPTIONS);
				
				Export.writeToFile(pms, WRITE_DESTINATION, OUTPUT_FILE_EXTENSION, WRITE_OPTIONS);

				logger.info("Finished writing files to " + new File(WRITE_DESTINATION).getAbsolutePath());
				
				//Export.writeToFile(oms, WRITE_DESTINATION, OUTPUT_FILE_EXTENSION, WRITE_OPTIONS);

			} catch ( Exception e1) {
			
				e1.printStackTrace();
			}
			
			//builtEnts.addAll(pms);
			
			//builtEnts.addAll(oms);
			
		} catch (Exception e) {
		
			logger.catching(Level.ERROR,e);
			
		} 

		logger.info("Job Completed");
	}
	
	private Collection<? extends Entity> processPatientEntities(Map<String, String> map) throws Exception {
		List<Entity> entities = new ArrayList<Entity>();
		PatientDimension pd = new PatientDimension("PatientDimension");
		pd.setPatientNum(map.containsKey("patientNum") ? map.get("patientNum") : null); 
		pd.setAgeInYearsNum(map.containsKey("ageInYearsNum") ? map.get("ageInYearsNum") : null); 
		pd.setSexCD(map.containsKey("sexCD") ? map.get("sexCD"): null);
		pd.setRaceCD(map.containsKey("raceCD") ? map.get("raceCD"): null);
		entities.add(pd);
		
		PatientTrial pt = new PatientTrial("PatientTrial");
		pt.setPatientNum(pd.getPatientNum());
		pt.setTrial(Entity.SOURCESYSTEM_CD);
		pt.setSecureObjectToken("EXP:PUBLIC");
		entities.add(pd);
		entities.add(pt);
		return entities;
	}

	private Map buildPatientRecordList(File file, List<PatientMapping2> mappings,
			Map<String, Map<String, String>> dataDict) throws  Exception {
		
		Map<String, String> patientMap = PatientMapping2.toMap(mappings);
		
		List<String> fileNames = PatientMapping2.getFileNames(mappings);	
		
		Map<String, Map<String, String>> patients = new LinkedHashMap<String,Map<String,String>>();
		if(file.isFile()) {
			
			if(!patientMap.containsKey("PatientNum")) {
				return new HashMap();
			}
			
			String patientNumFile = patientMap.get("PatientNum").split(":")[0];
			
			String sexCdFile = patientMap.containsKey("sexCD") ? patientMap.get("sexCD").split(":")[0] : "";
			String sexCdPatCol = "0";
			
			String ageInYearsNumFile =  patientMap.containsKey("ageInYearsNum") ? patientMap.get("ageInYearsNum").split(":")[0]: "";
			String ageInYearsNumPatCol = "0";
			
			String raceCDFile =  patientMap.containsKey("raceCD") ? patientMap.get("raceCD").split(":")[0] : "";
			String raceCDPatCol = "0";		
			if(patientNumFile.isEmpty()) {
				logger.info("No PatientNum given to generate patients.  Patients will not be generated.");
			} else {
				
				List recs = CSVDataSource2.buildObjectMap(patientNumFile, new File( Paths.get(FILE_NAME).getParent()+ "/" + patientNumFile  ), DATASOURCE_FORMAT);
				// check if file
				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("PatientNum"));
						if( values == null) throw new Exception(patientNumFile + "does not have values for Patient Ids.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );
						
						for(Object valueObj:values) {
							Map<String, String> newPat = new HashMap<String,String>();
	
							newPat.put("patientNum", valueObj.toString());
							patients.put(valueObj.toString(), newPat);
						}
					
					}
				}
			}
			if(!sexCdFile.isEmpty()) {

			List recs = CSVDataSource2.buildObjectMap(sexCdFile, new File( Paths.get(FILE_NAME).getParent() + "/" + sexCdFile ), DATASOURCE_FORMAT);

				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("sexCD"));
						if( values == null) throw new Exception(sexCdFile + "does not have values for Patients' sex.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );	
						Map<String,String> dict = dataDict.containsKey(sexCdFile) ? dataDict.get(sexCdFile): new HashMap<String,String>();
	
						for(Object valueObj:values) {
							Map<String, String> newPat = patients.get(defRec.get(patientMap.get("PatientNum")).get(0));
	
							valueObj = dict.containsKey(valueObj.toString()) ? dict.get(valueObj).toString(): valueObj;
							
							newPat.put("sexCD", valueObj.toString());
							
							patients.put(defRec.get(sexCdFile + ':' + sexCdPatCol).get(0).toString(), newPat);
						}
					}
				}
			}
			if(!ageInYearsNumFile.isEmpty()) {

				List recs = CSVDataSource2.buildObjectMap(ageInYearsNumFile, new File( Paths.get(FILE_NAME).getParent() + "/" + ageInYearsNumFile ), DATASOURCE_FORMAT);
	
				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("ageInYearsNum"));
						
						if( values == null) throw new Exception(ageInYearsNumFile + " does not have values for Patients' age.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );	
						
						Map<String,String> dict = dataDict.containsKey(ageInYearsNumFile) ? dataDict.get(ageInYearsNumFile): new HashMap<String,String>();
						if(!values.isEmpty()) {
							for(Object valueObj:values) {
								Map<String, String> newPat = patients.get(defRec.get(patientMap.get("PatientNum")).get(0));
		
								valueObj = dict.containsKey(valueObj.toString()) ? dict.get(valueObj).toString(): valueObj;
								
								newPat.put("ageInYearsNum", valueObj.toString());
								
								patients.put(defRec.get(ageInYearsNumFile + ':' + ageInYearsNumPatCol).get(0).toString(), newPat);
							}
						}
					}
				}	
			}
			if(!raceCDFile.isEmpty()) {
				List recs = CSVDataSource2.buildObjectMap(raceCDFile, new File( Paths.get(FILE_NAME).getParent() + "/"  + raceCDFile ), DATASOURCE_FORMAT);
	
				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("raceCD"));
	
						if( values == null) throw new Exception(raceCDFile + " does not have values for Patients' race.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );	
												
						Map<String,String> dict = dataDict.containsKey(raceCDFile) ? dataDict.get(raceCDFile): new HashMap<String,String>();
	
						for(Object valueObj:values) {
							Map<String, String> newPat = patients.get(defRec.get(patientMap.get("PatientNum")).get(0));
	
							valueObj = dict.containsKey(valueObj.toString()) ? dict.get(valueObj).toString(): valueObj;
							
							newPat.put("raceCD", valueObj.toString());
							
							patients.put(defRec.get(raceCDFile + ':' + raceCDPatCol).get(0).toString(), newPat);
						}
					}
				}			
			}
			
		} else if ( file.isDirectory() ) {
			
			if(!patientMap.containsKey("PatientNum")) {
				return new HashMap();
			}
			
			String patientNumFile = patientMap.get("PatientNum").split(":")[0];
			
			String sexCdFile = patientMap.containsKey("sexCD") ? patientMap.get("sexCD").split(":")[0] : "";
			String sexCdPatCol = "0";
			
			String ageInYearsNumFile =  patientMap.containsKey("ageInYearsNum") ? patientMap.get("ageInYearsNum").split(":")[0]: "";
			String ageInYearsNumPatCol = "0";
			
			String raceCDFile =  patientMap.containsKey("raceCD") ? patientMap.get("raceCD").split(":")[0] : "";
			String raceCDPatCol = "0";		
			
			if(patientNumFile.isEmpty()) {
				logger.info("No PatientNum given to generate patients.  Patients will not be generated.");
			} else {
				
				List recs = CSVDataSource2.buildObjectMap(patientNumFile, new File( FILE_NAME + patientNumFile  ), DATASOURCE_FORMAT);
				// check if file
				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("PatientNum"));
						if( values == null) throw new Exception(patientNumFile + "does not have values for Patient Ids.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );
						
						for(Object valueObj:values) {
							Map<String, String> newPat = new HashMap<String,String>();
	
							newPat.put("patientNum", valueObj.toString());
							patients.put(valueObj.toString(), newPat);
						}
					
					}
				}
			}
			if(!sexCdFile.isEmpty()) {

			List recs = CSVDataSource2.buildObjectMap(sexCdFile, new File( FILE_NAME + sexCdFile ), DATASOURCE_FORMAT);

				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("sexCD"));
						if( values == null) throw new Exception(sexCdFile + "does not have values for Patients' sex.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );	
						Map<String,String> dict = dataDict.containsKey(sexCdFile) ? dataDict.get(sexCdFile): new HashMap<String,String>();
	
						for(Object valueObj:values) {
							Map<String, String> newPat = patients.get(defRec.get(patientMap.get("PatientNum")).get(0));
	
							valueObj = dict.containsKey(valueObj.toString()) ? dict.get(valueObj).toString(): valueObj;
							
							newPat.put("sexCD", valueObj.toString());
							
							patients.put(defRec.get(sexCdFile + ':' + sexCdPatCol).get(0).toString(), newPat);
						}
					}
				}
			}
			if(!ageInYearsNumFile.isEmpty()) {

				List recs = CSVDataSource2.buildObjectMap(ageInYearsNumFile, new File( FILE_NAME + ageInYearsNumFile ), DATASOURCE_FORMAT);
	
				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("ageInYearsNum"));
						
						if( values == null) throw new Exception(ageInYearsNumFile + " does not have values for Patients' age.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );	
						
						Map<String,String> dict = dataDict.containsKey(ageInYearsNumFile) ? dataDict.get(ageInYearsNumFile): new HashMap<String,String>();
						if(!values.isEmpty()) {
							for(Object valueObj:values) {
								Map<String, String> newPat = patients.get(defRec.get(patientMap.get("PatientNum")).get(0));
		
								valueObj = dict.containsKey(valueObj.toString()) ? dict.get(valueObj).toString(): valueObj;
								
								newPat.put("ageInYearsNum", valueObj.toString());
								
								patients.put(defRec.get(ageInYearsNumFile + ':' + ageInYearsNumPatCol).get(0).toString(), newPat);
							}
						}
					}
				}	
			}
			if(!raceCDFile.isEmpty()) {
				List recs = CSVDataSource2.buildObjectMap(raceCDFile, new File( FILE_NAME + raceCDFile ), DATASOURCE_FORMAT);
	
				for(Object rec: recs) {
					if(rec instanceof LinkedHashMap) {
						
						LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
							
						List<Object> values = defRec.get( patientMap.get("raceCD"));
	
						if( values == null) throw new Exception(raceCDFile + " does not have values for Patients' race.  Ensure that you\n"
								+ " have proper column mapped and using correct delimiters." );	
												
						Map<String,String> dict = dataDict.containsKey(raceCDFile) ? dataDict.get(raceCDFile): new HashMap<String,String>();
	
						for(Object valueObj:values) {
							Map<String, String> newPat = patients.get(defRec.get(patientMap.get("PatientNum")).get(0));
	
							valueObj = dict.containsKey(valueObj.toString()) ? dict.get(valueObj).toString(): valueObj;
							
							newPat.put("raceCD", valueObj.toString());
							
							patients.put(defRec.get(raceCDFile + ':' + raceCDPatCol).get(0).toString(), newPat);
						}
					}
				}			
			}
		} else {
			return new HashMap();
		}
		//return fileNames;
		return patients;

	}
		
	private List buildRecordList(File file, List<Mapping> mappings, Map<String, Map<String,String>> dataDict) throws Exception{
		if(file.isFile()) {
			File f2 = file.getParentFile();
			return buildRecordList(f2,mappings,dataDict);
			
		} else if ( file.isDirectory() ) {
			IS_DIR = true;
			List records = new ArrayList();
			Set<String> filenames = new HashSet<String>();
			
			for(Mapping mapping: mappings) {
				filenames.add(mapping.getKey().split(":")[0]);
			}
			
			
			for(String f: filenames) {
				
				Map<String,String> dict = dataDict.containsKey(f) ? dataDict.get(f): new HashMap<String,String>();
	
				String fileName = f;
				List<File> files = Arrays.asList(file.listFiles());
				File fileToRead = null;

				for(File f2: files) {
					if(f2.getName().equals(fileName)) {
						fileToRead = f2;
						break;
					}
				}
				
				
				if(fileToRead != null || fileToRead.exists()) {
				
					List recs = CSVDataSource2.buildObjectMap(f, fileToRead, DATASOURCE_FORMAT);
					
					//if(!dataDict.isEmpty()) {
						
						for(Object rec:recs) {
							if(rec instanceof LinkedHashMap) {
								
								LinkedHashMap<String,List<Object>> defRec = (LinkedHashMap<String,List<Object>>) rec;
								
								for(String key:defRec.keySet()) {
									
									List<Object> values = defRec.get(key);
									List<Object> newValues = new ArrayList<Object>();
									
									for(Object valueObj:values) {
										String value = valueObj.toString().replaceAll("[*|\\\\\\/<\\?%>\":]", "");
										
										if(dict.containsKey(value)) {
											newValues.add(dict.get(value));
										} else {
											newValues.add(value);
										}
										
									}
									defRec.put(key, newValues);
								}
								records.add(defRec);
							}
							
						}
					//} else {
					//	records.addAll(recs);
					//}
				}
			}
			return records;
		} else {
			return new ArrayList();
		}
		
	}

	private Map<String, Map<String, String>> generateDataDict(File fileToRead) throws IOException {
		Map<String, Map<String, String>> returnmap = new HashMap<String, Map<String, String>>();
		
		au.com.bytecode.opencsv.CSVReader reader = new CSVReader(new FileReader((File) fileToRead), ',', '"','\0', 1);
		
		for(String[] rec: reader.readAll()) {
			if(returnmap.containsKey(rec[0])) {
				Map<String,String> dictMap = returnmap.get(rec[0]);
				dictMap.put(rec[1], rec[2]);
				returnmap.put(rec[0], dictMap);
			} else {
				Map<String,String> dictMap = new HashMap<String,String>();
				dictMap.put(rec[1], rec[2]);
				returnmap.put(rec[0], dictMap);
			}
		};
		
		return returnmap;
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
					
					String[] omkeys = omkeyfull.split(":");
					
					isOmitted = isOmitted(new ArrayList(Arrays.asList(omkeys)), record, omkeyfull);
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
							
							} else if ( o instanceof List) {
								l.addAll((List) o);
							} else if ( o instanceof String || o instanceof Boolean) {
								l.addAll(Arrays.asList(o));
							}
						} else {
							return (ArrayList) obj;
							
						}
						
					}

					return l;
						
				} 

			} 
			
		}
		
		return null;
	}
	
	// OMITS Records if omission key is given. 
	private List<Entity> processEntities(List<Mapping> mappings, Map record) throws Exception{

		List<Entity> builtEnts = new ArrayList<Entity>();
		
		List<Entity> entities = buildEntities();
		
		boolean isOmitted = isOmitted(record);
		
		/*
		for(String omkeyfull: OMISSIONS_MAP.keySet()) {
			
			String[] omkeys = omkeyfull.split(":");
			
			isOmitted = iterateOmkeys(Arrays.asList(omkeys), record);
			
		}
*/		
		
		if(!isOmitted){
			
			for(Mapping mapping: mappings){
				List<Object> relationalValue = new ArrayList<Object>();

				if(!IS_DIR) {
					
					String[] array = new String[mapping.getKey().split(":").length - 1];
					array[0] = mapping.getKey().split(":")[0] + ":" + RELATIONAL_KEY;
					relationalValue = findValueByKey(record,array);
				} else {
					
					String[] array = new String[mapping.getKey().split(":").length - 1];
					array[0] = mapping.getKey().split(":")[0] + ":" + RELATIONAL_KEY;
					relationalValue = findValueByKey(record,array);

				}
				
				DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));
				
				if(!mapping.getDataType().equalsIgnoreCase("OMIT")){
					List<Object> values = new ArrayList<Object>();
					
					if(!IS_DIR) {
						
						String[] array = new String[mapping.getKey().split(":").length - 1];
						array[0] = mapping.getKey();
						values = findValueByKey2(record,new ArrayList(Arrays.asList(array)));
						//values = findValueByKey2(record, new ArrayList(Arrays.asList(mapping.getKey().split(":"))));
					} else {
						
						
						String[] array = new String[mapping.getKey().split(":").length - 1];
						array[0] = mapping.getKey();
						values = findValueByKey2(record,new ArrayList(Arrays.asList(array)));
					}
					

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
					if(values == null) throw new Exception("Following Mapping does not exist in the datafile: \n"
							+ mapping.toCSV() + "\n" +
							" be sure that the column and file exist.");
					
					if(relationalValue == null) throw new Exception("Following Mapping does not exist in the datafile: \n"
							+ mapping.toCSV() + "\n" +
							" be sure that the column and file exist.");
					
					Set<Entity> newEnts = dt.generateTables(mapping, entities, values, relationalValue);
					
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
				
				List<Object> values = (ArrayList<Object>) findValueByKey2(new LinkedHashMap(record), new ArrayList(Arrays.asList(mapping.getPatientKey())));
				Map<String, String> options = mapping.buildOptions(mapping);
				
				if(!options.isEmpty()) {
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

			e.printStackTrace();
		
		}				
	}

	private void processEntitesMap(LinkedHashMap obj, String currKey, List<Entity> entities, String rELATIONAL_KEY2) {
		// TODO Auto-generated method stub
		
	}

	private List<Entity> buildEntities() throws InstantiationException, IllegalAccessException {
		
		return Entity.buildEntityList(ENTITY_PACKAGE, EXPORT_TABLES);
		
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
		
		if(jobProperties.containsKey("level1")) {
			LEVEL_1_ACCESS = jobProperties.get("level1").toString().equalsIgnoreCase("Y") ||
				jobProperties.get("level1").toString().equalsIgnoreCase("YES") ||
				jobProperties.get("level1").toString().equalsIgnoreCase("TRUE") ? true: false;
		}
		//required
		FILE_NAME = jobProperties.getProperty("filename"); ; 
		
		WRITE_DESTINATION = jobProperties.getProperty("writedestination"); 
				
		MAPPING_FILE = jobProperties.getProperty("mappingfile"); 
		
		PATIENT_MAPPING_FILE = jobProperties.getProperty("patientmappingfile");
		//optional
		MAPPING_DELIMITER = jobProperties.containsKey("mappingdelimiter") && jobProperties.getProperty("mappingdelimiter").toCharArray().length == 1 ? 
				jobProperties.getProperty("mappingdelimiter").toCharArray()[0] : ',';
				
		if(jobProperties.containsKey("relationalkey")){
			RELATIONAL_KEY = jobProperties.getProperty("relationalkey");
		} else {
			RELATIONAL_KEY_OVERRIDE = false;
		}
		Entity.SOURCESYSTEM_CD = jobProperties.containsKey("sourcesystemcd") ? jobProperties.getProperty("sourcesystemcd"): "TRIAL";
		
		CSVDataSource2.SKIP_HEADER = jobProperties.containsKey("skipdataheader") && jobProperties.getProperty("skipdataheader").equalsIgnoreCase("Y") ? 1:0;  
		
		CSVDataSource2.DELIMITER = jobProperties.containsKey("datadelimiter") ? jobProperties.getProperty("datadelimiter").toCharArray()[0]
				: ',';  
		
		DATASOURCE_FORMAT = jobProperties.containsKey("datasourceformat") ? Class.forName(jobProperties.getProperty("datasourceformat")): null;
	
		DICT_FILE = jobProperties.containsKey("dictfile") ? 
				new File(jobProperties.getProperty("dictfile")): new File("");
	
		MAPPING_QUOTED_STRING = jobProperties.containsKey("mappingquotedstring") ? 
				jobProperties.getProperty("mappingquotedstring").charAt(0): '"';
				
		MAPPING_SKIP_HEADER = jobProperties.containsKey("skipmapperheader") &&
				jobProperties.getProperty("skipmapperheader").substring(0, 1).equalsIgnoreCase("Y") ? true: false;
		
		//sequencers = jobProperties.containsKey("sequencers") ? buildSequences(jobProperties.getProperty("sequencers")): sequencers;
	
		CONCEPT_CD_STARTING_SEQ = jobProperties.containsKey("conceptcdstartseq") ? new Integer(jobProperties.get("conceptcdstartseq").toString()): 1;
		
		ENCOUNTER_NUM_STARTING_SEQ = jobProperties.containsKey("encounternumstartseq") ? new Integer(jobProperties.get("encounternumstartseq").toString()): 1;
		PATIENT_NUM_STARTING_SEQ = jobProperties.containsKey("patientnumstartseq") ? new Integer(jobProperties.get("patientnumstartseq").toString()): 1;

		DO_SEQUENCING = jobProperties.containsKey("sequencedata") ? jobProperties.get("sequencedata").toString().toUpperCase().contains("Y") ? true: false: true;
		DO_PATIENT_NUM_SEQUENCE = jobProperties.containsKey("sequencepatient") ? jobProperties.get("sequencepatient").toString().toUpperCase().contains("Y") ? true: false: true;
		DO_CONCEPT_CD_SEQUENCE = jobProperties.containsKey("sequenceconcept") ? jobProperties.get("sequenceconcept").toString().toUpperCase().contains("Y") ? true: false: true;
		DO_ENCOUNTER_NUM_SEQUENCE = jobProperties.containsKey("sequenceencounter") ? jobProperties.get("sequenceencounter").toString().toUpperCase().contains("Y") ? true: false: true;
		DO_INSTANCE_NUM_SEQUENCE = jobProperties.containsKey("sequenceinstance") ? jobProperties.get("sequenceinstance").toString().toUpperCase().contains("Y") ? true: false: true;
		
	}
	
	
	
	private List<ColumnSequencer> buildSequences(String sequences) {
		String[] seqsSplit = sequences.split("\\],\\[");
		//("ObservationFact"),"encounterNum","ENCNUM","I2B2",1,1
		List<ColumnSequencer> list = new ArrayList<ColumnSequencer>();
		for(String s:seqsSplit) {
			String[] inner = s.split("\\),");
			String[] tables = inner[0].replaceFirst("\\[\\(", "").split(",");
			String[] configs = inner[1].split(",");
			ColumnSequencer cs = new ColumnSequencer(Arrays.asList(tables), configs[0], configs[1], configs[2], new Integer(configs[3]), new Integer(configs[4].replaceAll("\\]","")));
		}
		
		return list;
	}

	private Collection<? extends Entity> thisFillTree(Set<Entity> entities) throws Exception {
		List<I2B2> i2b2 = new ArrayList<I2B2>();
		// fill in tree
		if(EXPORT_TABLES.contains("I2B2")){
		
			for(Entity entity: entities){
				
				if(entity instanceof I2B2){
					I2B2 i = (I2B2) entity;
					if(i.getcTableName().equalsIgnoreCase("CONCEPT_DIMENSION") ) {
						i2b2.add((I2B2) entity);
					}
				}
				
			}
			
		}
		
		return I2B2.fillTree(i2b2);
	}
}
