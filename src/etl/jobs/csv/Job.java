package etl.jobs.csv;

import static etl.jobs.csv.Job.PATIENT_MAPPING_FILE;

import org.apache.commons.lang3.StringUtils;

import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

public abstract class Job {
	
	protected static boolean SKIP_HEADERS = true;

	protected static String WRITE_DIR = "./completed/";
	
	protected static String MAPPING_FILE = "./mappings/mapping.csv";

	protected static boolean MAPPING_SKIP_HEADER = true;

	protected static char MAPPING_DELIMITER = ',';

	protected static char MAPPING_QUOTED_STRING = '"';

	protected static final char DATA_SEPARATOR = ',';

	protected static final char DATA_QUOTED_STRING = '"';
	
	protected static String DATA_DIR = "./data/";

	protected static String TRIAL_ID = "DEFAULT";

	protected static CharSequence PATH_SEPARATOR = "\\";

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

	// Metadata for Numerics
	protected static final String C_METADATAXML_NUMERIC = "<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>";
	
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
			DATA_DIR = properties.contains("datadir") ? properties.getProperty("datadir").toString() : DATA_DIR;
			WRITE_DIR = properties.contains("writedir") ? properties.getProperty("writedir").toString() : WRITE_DIR;
			
			if(properties.contains("dataskipheaders")) {
				if(new String(StringUtils.substring(properties.getProperty("dataskipheaders"),0,1)).equalsIgnoreCase("N")){
					SKIP_HEADERS = false;
				}
			}
			///////////////////////
			// Mapping Variables //
			if(properties.contains("mappingskipheaders")) {
				if(new String(StringUtils.substring(properties.getProperty("mappingskipheaders"),0,1)).equalsIgnoreCase("N")){
					MAPPING_SKIP_HEADER = false;
				}
			}
			
			PATIENT_MAPPING_FILE = properties.contains("patientmappingfile") ? properties.getProperty("patientmappingfile").toString() : PATIENT_MAPPING_FILE;
			
			
			MAPPING_QUOTED_STRING = properties.contains("mappingquotedstring") ? properties.getProperty("mappingquotedstring").toCharArray()[0] : MAPPING_QUOTED_STRING;
			MAPPING_DELIMITER = properties.contains("mappingdelimiter") ? properties.getProperty("mappingdelimiter").toCharArray()[0] : MAPPING_DELIMITER;
			MAPPING_FILE = properties.contains("mappingfile") ? properties.getProperty("mappingfile").toString() : MAPPING_FILE;
	
			////////////////////
			// Misc Variables //
			TRIAL_ID = properties.contains("trialid") ? properties.getProperty("trialid").toString() : TRIAL_ID;
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
			
			CONCEPT_CD_STARTING_SEQ = properties.contains("concepstartseqnum") ? Integer.valueOf(properties.getProperty("concepstartseqnum")) : CONCEPT_CD_STARTING_SEQ;
			ENCOUNTER_NUM_STARTING_SEQ = properties.contains("encounterstartseqnum") ? Integer.valueOf(properties.getProperty("encounterstartseqnum")) : ENCOUNTER_NUM_STARTING_SEQ;
			PATIENT_NUM_STARTING_SEQ = properties.contains("patientstartseqnum") ? Integer.valueOf(properties.getProperty("patientstartseqnum")) : PATIENT_NUM_STARTING_SEQ;
			INSTANCE_NUM_STARTING_SEQ = properties.contains("instancestartseqnum") ? Integer.valueOf(properties.getProperty("instancestartseqnum")) : INSTANCE_NUM_STARTING_SEQ;
		}
		/**
		 *  Flags override all settings
		 */
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-skipheaders")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("N")) {
					SKIP_HEADERS = false;
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
			if(arg.equalsIgnoreCase( "-patientmappingfile" )){
				PATIENT_MAPPING_FILE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-mappingfile" )){
				MAPPING_FILE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
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
			if(arg.equalsIgnoreCase("-patientstartseqnum")){
				PATIENT_NUM_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			if(arg.equalsIgnoreCase("-concepstartseqnum")){
				CONCEPT_CD_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			if(arg.equalsIgnoreCase("-encounterstartseqnum")){
				ENCOUNTER_NUM_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			if(arg.equalsIgnoreCase("-instancestartseqnum")){
				INSTANCE_NUM_STARTING_SEQ = Integer.valueOf(checkPassedArgs(arg, args));
			}
			
		}		
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
	
	public static JobProperties buildProperties(String[] args) throws Exception {
		String propPath = "";
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-propertiesfile")){
				propPath = Utils.checkPassedArgs(arg, args);
			}
		}
		return null;
	}
}