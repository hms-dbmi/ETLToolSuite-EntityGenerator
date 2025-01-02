package etl.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

public abstract class Job implements Serializable {
	
	/**
	 * 
	 */
	protected static String METADATA_TYPE = "BDC";
	
	protected static String JOB_TYPE = "BDC";
	
	private static final long serialVersionUID = -4280736713043802649L;

	protected static String MANAGED_INPUT = "data/Managed_Inputs.csv";

	protected static String GENOMIC_MANAGED_INPUT = "data/Genomic_Managed_Inputs.csv";
	
	protected static boolean SKIP_HEADERS = false;

	protected static String WRITE_DIR = "./completed/";

	protected static String LOG_DIR = "./logs/";
	
	protected static String RESOURCE_DIR = "./resources/";

	protected static String PROCESSING_FOLDER = "./processing/";
	
	protected static String MAPPING_FILE = "./mappings/mapping.csv";

	protected static String MAPPING_DIR = "./mappings/";
	
	protected static boolean MAPPING_SKIP_HEADER = false;

	protected static char MAPPING_DELIMITER = ',';

	protected static char MAPPING_QUOTED_STRING = '"';

	protected static char DATA_SEPARATOR = ',';

	protected static char DATA_QUOTED_STRING = '"';
	
	public static String DATA_DIR = "./data/";

	protected static String DICT_DIR = "./dict/";
	
	protected static String TRIAL_ID = "DEFAULT";
	
	protected static CharSequence PATH_SEPARATOR = "µ";

	protected  static String ROOT_NODE = PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR;
	
	protected static Integer PATIENT_COL = 0;

	// Sequencing Variables
	protected static boolean DO_SEQUENCING = true;
	
	protected static boolean DO_PATIENT_NUM_SEQUENCE = true;
	
	protected static boolean DO_CONCEPT_CD_SEQUENCE = true;
	
	protected static boolean DO_ENCOUNTER_NUM_SEQUENCE = true;
	
	protected static boolean DO_INSTANCE_NUM_SEQUENCE = true;
	
	protected static Integer CONCEPT_CD_STARTING_SEQ = 1;

	protected static Integer ENCOUNTER_NUM_STARTING_SEQ = 1;

	protected static Integer PATIENT_NUM_STARTING_SEQ = 1;
	
	protected static Integer INSTANCE_NUM_STARTING_SEQ = 1;

	protected static String PATIENT_MAPPING_FILE = "./mappings/mapping.csv.patient";

	protected static boolean IS_SEGMENTED = false;
	
	protected static String CONFIG_FILENAME = "";
	
	protected static String FACT_FILENAME = "ObservationFact.csv";
	// Metadata for Numerics
	protected static String C_METADATAXML_NUMERIC = "<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>";

	/**
	 * Flags override properties.
	 * @param args
	 * @throws Exception
	 */
	protected static void setVariables(String[] args, JobProperties properties) throws Exception {
		if(properties != null) {
			////////////////////////////
			// build from config file //
			//////////////////////////////////
			// File and Directory Variables //
			MANAGED_INPUT = properties.contains("managedinputsfile") ? properties.getProperty("managedinputsfile").toString() : MANAGED_INPUT;
			
			GENOMIC_MANAGED_INPUT = properties.contains("genomicmanagedinputsfile")
					? properties.getProperty("genomicmanagedinputsfile").toString()
					: GENOMIC_MANAGED_INPUT;

			DATA_DIR = properties.contains("datadir") ? properties.getProperty("datadir").toString() : DATA_DIR;

			DICT_DIR = properties.contains("dictdir") ? properties.getProperty("dictdir").toString() : DICT_DIR;
			
			WRITE_DIR = properties.contains("writedir") ? properties.getProperty("writedir").toString() : WRITE_DIR;
			RESOURCE_DIR = properties.contains("resourcedir") ? properties.getProperty("resourcedir").toString() : RESOURCE_DIR;

			if(properties.contains("segmentfacts")) {
				if(new String(StringUtils.substring(properties.getProperty("segmentfacts"),0,1)).equalsIgnoreCase("Y")){
					IS_SEGMENTED = false;
				}
			}
			
			if(properties.contains("skipdataheader")) {
				if(new String(StringUtils.substring(properties.getProperty("skipdataheader"),0,1)).equalsIgnoreCase("N")){
					SKIP_HEADERS = false;
				} else {
					SKIP_HEADERS = true;
				}
			}
			///////////////////////
			// Mapping Variables //
			if(properties.contains("skipmapperheader")) {
				if(new String(StringUtils.substring(properties.getProperty("skipmapperheader"),0,1)).equalsIgnoreCase("N")){
					MAPPING_SKIP_HEADER = false;
				}
			}
			
			PATIENT_MAPPING_FILE = properties.contains("patientmappingfile") ? properties.getProperty("patientmappingfile").toString() : PATIENT_MAPPING_FILE;
			
			DATA_QUOTED_STRING = properties.contains("dataquotedstring") ? properties.getProperty("dataquotedstring").toCharArray()[0] : DATA_QUOTED_STRING;
			DATA_SEPARATOR = properties.contains("datadelimiter") ? properties.getProperty("datadelimiter").toCharArray()[0] : DATA_SEPARATOR;
			
			MAPPING_QUOTED_STRING = properties.contains("mappingquotedstring") ? properties.getProperty("mappingquotedstring").toCharArray()[0] : MAPPING_QUOTED_STRING;
			MAPPING_DELIMITER = properties.contains("mappingdelimiter") ? properties.getProperty("mappingdelimiter").toCharArray()[0] : MAPPING_DELIMITER;
			MAPPING_FILE = properties.contains("mappingfile") ? properties.getProperty("mappingfile").toString() : MAPPING_FILE;
	
			////////////////////
			// Misc Variables //
			TRIAL_ID = properties.contains("trialid") ? properties.getProperty("trialid").toString() : TRIAL_ID;
			
			ROOT_NODE = properties.contains("rootnode") ? PATH_SEPARATOR + properties.getProperty("rootnode").toString() + PATH_SEPARATOR: ROOT_NODE;
						
			PATH_SEPARATOR = properties.contains("pathseparator") ? new StringBuilder(properties.getProperty("pathseparator")) : PATH_SEPARATOR;

			//////////////////////////
			// Sequencing variables //
			if(properties.contains("sequencedata")) {
				if(new String(StringUtils.substring(properties.getProperty("sequencedata"),0,1)).equalsIgnoreCase("N")){
					DO_SEQUENCING = false;
				}
			}
			if(properties.contains("sequencepatient")) {
				if(new String(StringUtils.substring(properties.getProperty("sequencepatient"),0,1)).equalsIgnoreCase("N")){
					DO_PATIENT_NUM_SEQUENCE = false;
				}
			}
			if(properties.contains("sequenceconcept")) {
				if(new String(StringUtils.substring(properties.getProperty("sequenceconcept"),0,1)).equalsIgnoreCase("N")){
					DO_CONCEPT_CD_SEQUENCE = false;
				}
			}
			if(properties.contains("sequenceencounter")) {
				if(new String(StringUtils.substring(properties.getProperty("sequenceencounter"),0,1)).equalsIgnoreCase("N")){
					DO_ENCOUNTER_NUM_SEQUENCE = false;
				}
			}
			if(properties.contains("sequenceinstance")) {
				if(new String(StringUtils.substring(properties.getProperty("sequenceinstance"),0,1)).equalsIgnoreCase("N")){
					DO_INSTANCE_NUM_SEQUENCE = false;
				}
			}
			if(properties.contains("patientcol")) {
				
				PATIENT_COL = new Integer(properties.get("patientcol").toString());
				
				
			}
			CONCEPT_CD_STARTING_SEQ = properties.contains("conceptcdstartseq") ? Integer.valueOf(properties.getProperty("conceptcdstartseq")) : CONCEPT_CD_STARTING_SEQ;
			ENCOUNTER_NUM_STARTING_SEQ = properties.contains("encounternumstartseq") ? Integer.valueOf(properties.getProperty("encounternumstartseq")) : ENCOUNTER_NUM_STARTING_SEQ;
			PATIENT_NUM_STARTING_SEQ = properties.contains("patientnumstartseq") ? Integer.valueOf(properties.getProperty("patientnumstartseq")) : PATIENT_NUM_STARTING_SEQ;
			INSTANCE_NUM_STARTING_SEQ = properties.contains("instancenumstartseq") ? Integer.valueOf(properties.getProperty("instancenumstartseq")) : INSTANCE_NUM_STARTING_SEQ;
		}
		/**
		 *  Flags override all settings
		 */
		for(String arg: args) {
			if(arg.equalsIgnoreCase("patientcol")) {
				
				PATIENT_COL = new Integer(checkPassedArgs(arg, args));
				
			}
			if(arg.equalsIgnoreCase("-skipheaders")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					SKIP_HEADERS = false;
				} else {
					SKIP_HEADERS = true;
				}
			}
			if(arg.equalsIgnoreCase( "-mappingskipheaders" )){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					MAPPING_SKIP_HEADER = false;
				} 
			}
			if(arg.equalsIgnoreCase( "-mappingquotedstring" )){
				String qs = checkPassedArgs(arg, args);
				MAPPING_QUOTED_STRING = qs.charAt(0);
			}
			if(arg.equalsIgnoreCase( "-mappingdelimiter" )){
				String md = checkPassedArgs(arg, args);
				MAPPING_DELIMITER = md.charAt(0);
			}
			if(arg.equalsIgnoreCase( "-metadatatype" )){
				METADATA_TYPE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-patientmappingfile" )){
				PATIENT_MAPPING_FILE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-mappingfile" )){
				MAPPING_FILE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-resourcedir" )){
				RESOURCE_DIR = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
			}
			if(arg.equalsIgnoreCase( "-rootnode" )){
				ROOT_NODE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-writedir" )){
				WRITE_DIR = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-trialid" )){
				TRIAL_ID  = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-pathseparator" )){
				PATH_SEPARATOR  = checkPassedArgs(arg, args);
			} 
			// Seqeunce Variables
			if(arg.equalsIgnoreCase("-sequencedata")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					DO_SEQUENCING = false;
				} 
			}
			if(arg.equalsIgnoreCase("-sequencepatient")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					DO_PATIENT_NUM_SEQUENCE = false;
				} 
			}
			if(arg.equalsIgnoreCase("-sequenceconcept")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					DO_CONCEPT_CD_SEQUENCE = false;
				} 
			}
			if(arg.equalsIgnoreCase("-sequenceencounter")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					DO_ENCOUNTER_NUM_SEQUENCE = false;
				} 
			}
			if(arg.equalsIgnoreCase("-sequenceinstance")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					DO_INSTANCE_NUM_SEQUENCE = false;
				} 
			}
			if(arg.equalsIgnoreCase("-patientnumstartseq")){
				PATIENT_NUM_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			if(arg.equalsIgnoreCase("-conceptcdstartseq")){
				CONCEPT_CD_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			if(arg.equalsIgnoreCase("-encounternumstartseq")){
				ENCOUNTER_NUM_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			if(arg.equalsIgnoreCase("-instancenumstartseq")){
				INSTANCE_NUM_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			
		}		
	}

	public static JobProperties buildProperties(String[] args) throws Exception {
		String propPath = "";
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-propertiesfile")){
				Utils.checkPassedArgs(arg, args);
				CONFIG_FILENAME = Paths.get(Utils.checkPassedArgs(arg, args)).getFileName().toString();
				propPath = Utils.checkPassedArgs(arg, args);
			}
		}
		if(propPath.isEmpty()) {
			return null;
		}
		if(!Files.exists(Paths.get(propPath))){
			return null;
		};
		return JobProperties.class.newInstance().buildProperties(propPath);
	}

	// checks passed arguments and sends back value for that argument
	public static String checkPassedArgs(String arg, String[] args) throws Exception {
		
		int argcount = 0;
		
		String argv = new String();
		
		for(String thisarg: args) {
			
			if(thisarg.equals(arg)) {
				
				break;
				
			} else {
				
				argcount++;
				
			}
		}
		
		if(args.length > argcount) {
			
			argv = args[argcount + 1];
			
		} else {
			
			throw new Exception("Error in argument: " + arg );
			
		}
		return argv;
	}
	

	/**
	 * Builds Patient mappings into a list of arrays
	 * uses the managedInputs to control which patientMappings are being generated
	 * @param managedInputs
	 * @return
	 * @throws IOException
	 */
	protected static Map<String,Map<String,String>> getPatientMappings(List<ManagedInput> managedInputs) throws IOException {
		Map<String,Map<String,String>> returnPMMap = new HashMap<String,Map<String,String>>();
		
		Set<String> studyIds = new TreeSet<String>();
		
		
		for(ManagedInput managedInput: managedInputs) {
			if(!managedInput.getReadyToProcess().toLowerCase().startsWith("y")) continue;
			
			String studyid = managedInput.getStudyAbvName();
			
			studyIds.add(studyid);
			
		}
		
		for(String studyid: studyIds) {
			if(!Files.exists(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))) {
				System.err.println(studyid.toUpperCase() + " study is missing.  Make sure the shortname for trial id is correct in managed inputs.");
				continue;
			}
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))){
				
				try (CSVReader reader = new CSVReader(buffer, ',', '\"', '√')) {
					String line[];
					
					
					
					while((line = reader.readNext()) != null) {
						if(line.length < 3) continue;
						if(returnPMMap.containsKey(studyid)) {
							returnPMMap.get(studyid).put(line[0], line[2]);
						} else {
							
							Map<String,String> innermap = new HashMap<>();
							innermap.put(line[0], line[2]);
							returnPMMap.put(studyid,innermap);
						}
					}
				}
			}
		}
		
		return returnPMMap;
	}
	/**
	 * 
	 */
	/**
	 * Builds Patient mappings into a list of arrays
	 * uses the managedInputs to control which patientMappings are being generated
	 * @param managedInputs
	 * @return
	 * @throws IOException
	 */
	protected static List<String[]> getPatientMappingsOld(List<ManagedInput> managedInputs) throws IOException {
		List<String[]> returnPMSet = new ArrayList<>();
		
		for(ManagedInput managedInput: managedInputs) {
			String studyid = managedInput.getStudyAbvName();
			
			if(!Files.exists(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))) {
				System.err.println(studyid.toUpperCase() + " study is missing.  Make sure the shortname for trial id is correct in managed inputs.");
				continue;
			}
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))){
				
				try (CSVReader reader = new CSVReader(buffer, ',', '\"', '√')) {
					String line[];
					
					while((line = reader.readNext()) != null) {
						if(line[0].equalsIgnoreCase("SOURCE_ID")) continue;
						returnPMSet.add(line);
					}
				}
			}
		}
		
		return returnPMSet;
	}
	/**
	 * Builds Patient mappings into a list of arrays
	 * uses the managedInputs to control which patientMappings are being generated
	 * @param managedInputs
	 * @return
	 * @throws IOException
	 */
	protected static List<String[]> getPatientMappings(String studyAbvName) throws IOException {
		List<String[]> returnPMSet = new ArrayList<>();
		
		String studyid = studyAbvName.toUpperCase();
		
		if(!Files.exists(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))) {
			System.err.println(studyid.toUpperCase() + " study is missing.  Make sure the shortname for trial id is correct in managed inputs.");
		}
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))){
			
			try (CSVReader reader = new CSVReader(buffer, ',', '\"', '√')) {
				String line[];
				
				while((line = reader.readNext()) != null) {
					if(line[0].equalsIgnoreCase("SOURCE_ID")) continue;
					returnPMSet.add(line);
				}
			}
		}
		
		
		return returnPMSet;
	}
	/**
	 * Will look up the hpds id using the and study abv name
	 * 
	 * @param sourceId
	 * @param studyAbvName
	 * @param map
	 * @return
	 */
	
	
	protected static char[] toCsv(String[] line) {
		StringBuilder sb = new StringBuilder();
		
		int lastNode = line.length - 1;
		int x = 0;
		for(String node: line) {
			
			sb.append('"');
			sb.append(node);
			sb.append('"');
			
			if(x == lastNode) {
				sb.append('\n');
			} else {
				sb.append(',');
			}
			x++;
		}
		
		return sb.toString().toCharArray();
	}
	protected static String arrToCsv(String[] line) {
		
		return String.join(",", line);
	}

	/**
	 * Will look up the hpds id using the and study abv name
	 * 
	 * @param sourceId
	 * @param studyAbvName
	 * @param map
	 * @return
	 */
	public static String mappingLookup(String sourceId, Map<String, String> map) {
		Entry<String,String> result = map.entrySet().parallelStream()
				.filter(pm -> pm.getKey().equals(sourceId))
				.findFirst()
				.orElse(null);
		/*for(String[] pm: patientMappings) {
			if(pm[0].equals(sourceId)) {
				return pm[2];
			}
		}*/
		if(result == null) return null;
		return result.getValue();
		
	}
}
