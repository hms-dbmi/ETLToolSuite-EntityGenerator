package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.job.entity.i2b2tm.PatientDimension;
import etl.job.entity.i2b2tm.PatientMapping;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * 
 * Generates sequences for the columns in the data file.
 *
 */
public class Sequencer {
	private static boolean SKIP_HEADERS = false;

	private static String DATA_DIR = "./completed/";
	
	private static char DATA_SEPARATOR = ',';

	private static char DATA_QUOTED_STRING = '"';
		
	private static String TRIAL_ID = "DEFAULT";
	
	private static boolean DO_SEQUENCING = true;
	
	private static boolean DO_PATIENT_NUM_SEQUENCE = true;
	
	private static boolean DO_CONCEPT_CD_SEQUENCE = true;
	
	private static boolean DO_ENCOUNTER_NUM_SEQUENCE = true;
	
	private static boolean DO_INSTANCE_NUM_SEQUENCE = true;
	
	private static Integer CONCEPT_CD_STARTING_SEQ = 1;

	private static Integer ENCOUNTER_NUM_STARTING_SEQ = 1;

	private static Integer PATIENT_NUM_STARTING_SEQ = 1;
	
	private static Integer INSTANCE_NUM_STARTING_SEQ = 1;
	
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			if(DO_SEQUENCING) execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 *  PATIENT NUM SEQUENCED IN OBSERVATIONFACT.CSV AND PATIENTDIMENSION.CSV
	 *  WILL ALSO GENERATE THE PATIENT_MAPPING IS PATIENT NUM IS SEQUENCED.  
	 *  IF THIS IS NOT SEQUENCE PATIENT_MAPPING WILL NOT BE GENERATED.
	 */
	private static void execute() {

		if(DO_PATIENT_NUM_SEQUENCE) {
			try {
				sequencePatients();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(DO_CONCEPT_CD_SEQUENCE) {
			try {
				try {
					sequenceConcepts();
				} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private static void sequenceConcepts() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		Map<String,String> conceptMap = new HashMap<String,String>();
		List<ConceptDimension> concepts = new ArrayList<ConceptDimension>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
			CsvToBean<ConceptDimension> csvToBean = 
					Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
			concepts = csvToBean.parse();
			
			concepts.forEach(concept ->{
				
				conceptMap.put(concept.getConceptCd(), String.valueOf(CONCEPT_CD_STARTING_SEQ));

				concept.setConceptCd(String.valueOf(CONCEPT_CD_STARTING_SEQ));
				
				CONCEPT_CD_STARTING_SEQ++;
			});
			
		}
			
		List<ObservationFact> facts = new ArrayList<ObservationFact>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){
			
			CsvToBean<ObservationFact> csvToBean = 
					Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);

			
			facts = csvToBean.parse();
			
			facts.stream().forEach(fact -> {
				if(!conceptMap.containsKey(fact.getConceptCd())) {
					if(!fact.getConceptCd().equalsIgnoreCase("SECURITY")) 
						System.err.println("Concept ( " + fact.getConceptCd() + " )  does not exist in concpept dimension");
					return;
			 	}
				String conceptCd = conceptMap.get(fact.getConceptCd());
				
				fact.setConceptCd(conceptCd);
			});
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
			Utils.writeToCsv(buffer, concepts, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){
			
			Utils.writeToCsv(buffer, facts, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 

	}

	private static void sequencePatients() throws Exception {
		// LOAD ALL POSSIBLE PATIENT NUMS INTO A MAP<STRING,STRING> 
		Map<String,String> patientMap = new HashMap<String,String>();
		List<PatientDimension> patients = new ArrayList<PatientDimension>();
		List<PatientMapping> patientMapppings = new ArrayList<PatientMapping>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "PatientDimension.csv"))){
			
			CsvToBean<PatientDimension> csvToBean = 
					Utils.readCsvToBean(PatientDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
			patients = csvToBean.parse();
									
			patients.forEach(pd ->{
				
				patientMap.put(pd.getPatientNum(), String.valueOf(PATIENT_NUM_STARTING_SEQ));
				
				String sourcePatNum = pd.getPatientNum();
				String mappedPatNum = String.valueOf(PATIENT_NUM_STARTING_SEQ);
				
				pd.setPatientNum(mappedPatNum);
				pd.setSourceSystemCD(TRIAL_ID + ":" + PATIENT_NUM_STARTING_SEQ);
				// Create mapping
				PatientMapping pm = new PatientMapping();
				
				pm.setPatientIde(sourcePatNum);
				pm.setPatientIdeSource(TRIAL_ID);
				pm.setPatientNum(mappedPatNum);
				patientMapppings.add(pm);
				
				PATIENT_NUM_STARTING_SEQ++;
			});
		}

		List<ObservationFact> facts = new ArrayList<ObservationFact>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){
			
			CsvToBean<ObservationFact> csvToBean = 
					Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);

			
			facts = csvToBean.parse();
			
			for(ObservationFact fact: facts) {
				if(!patientMap.containsKey(fact.getPatientNum())) {
					System.err.println("Patient Num ( " + fact.getPatientNum() + " )  does not exist in patient dimension");
					continue;
				}
				String patientNum = patientMap.get(fact.getPatientNum());
				
				fact.setPatientNum(patientNum);
			}
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "PatientDimension.csv"))){
			
			Utils.writeToCsv(buffer, patients, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){
			
			Utils.writeToCsv(buffer, facts, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "PatientMapping.csv"))){
			
			Utils.writeToCsv(buffer, patientMapppings, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
	}

	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-skipheaders")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					SKIP_HEADERS = true;
				} 
			}

			if(arg.equalsIgnoreCase( "-dataseparator" )){
				DATA_SEPARATOR = checkPassedArgs(arg, args).charAt(0);
			} 

			if(arg.equalsIgnoreCase( "-dataquotedstring" )){
				DATA_QUOTED_STRING = checkPassedArgs(arg, args).charAt(0);
			} 
			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
			} 
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
}