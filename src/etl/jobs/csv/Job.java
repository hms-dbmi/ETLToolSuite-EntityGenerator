package etl.jobs.csv;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;

import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

public abstract class Job implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4280736713043802649L;

	protected static boolean SKIP_HEADERS = true;

	protected static String WRITE_DIR = "./completed/";
	
	protected static String RESOURCE_DIR = "./resources/";

	protected static String PROCESSING_FOLDER = "./processing/";
	
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

	protected static boolean IS_SEGMENTED = false;
	
	protected static String CONFIG_FILENAME = "";
	
	protected static String FACT_FILENAME = "ObservationFact.csv";
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
			RESOURCE_DIR = properties.contains("resourcedir") ? properties.getProperty("resourcedir").toString() : RESOURCE_DIR;

			if(properties.contains("segmentfacts")) {
				if(new String(StringUtils.substring(properties.getProperty("segmentfacts"),0,1)).equalsIgnoreCase("Y")){
					IS_SEGMENTED = false;
				}
			}
			
			if(properties.contains("skipdataheader")) {
				if(new String(StringUtils.substring(properties.getProperty("skipdataheader"),0,1)).equalsIgnoreCase("N")){
					SKIP_HEADERS = false;
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
			
			CONCEPT_CD_STARTING_SEQ = properties.contains("conceptcdstartseq") ? Integer.valueOf(properties.getProperty("conceptcdstartseq")) : CONCEPT_CD_STARTING_SEQ;
			ENCOUNTER_NUM_STARTING_SEQ = properties.contains("encounternumstartseq") ? Integer.valueOf(properties.getProperty("encounternumstartseq")) : ENCOUNTER_NUM_STARTING_SEQ;
			PATIENT_NUM_STARTING_SEQ = properties.contains("patientnumstartseq") ? Integer.valueOf(properties.getProperty("patientnumstartseq")) : PATIENT_NUM_STARTING_SEQ;
			INSTANCE_NUM_STARTING_SEQ = properties.contains("instancenumstartseq") ? Integer.valueOf(properties.getProperty("instancenumstartseq")) : INSTANCE_NUM_STARTING_SEQ;
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
			if(arg.equalsIgnoreCase( "-resourcedir" )){
				RESOURCE_DIR = checkPassedArgs(arg, args);
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
}
