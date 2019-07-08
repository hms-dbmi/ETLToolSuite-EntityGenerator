package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.job.entity.i2b2tm.PatientDimension;
import etl.job.entity.i2b2tm.PatientMapping;
import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * 
 * Generates sequences for the columns in the data file.
 *
 */
public class CEIsequencer extends Job{

	
	public static void main(String[] args, Collection<ObservationFact> facts, Collection<ConceptDimension> setCds, JobProperties buildProperties ) {
		try {
			setVariables(args, buildProperties);
		} catch (Exception e) {
			System.err.println(e);
		}
		
		try {
			if(DO_SEQUENCING) execute(facts, setCds);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		}
	}
	/**
	 *  PATIENT NUM SEQUENCED IN OBSERVATIONFACT.CSV AND PATIENTDIMENSION.CSV
	 *  WILL ALSO GENERATE THE PATIENT_MAPPING IS PATIENT NUM IS SEQUENCED.  
	 *  IF THIS IS NOT SEQUENCE PATIENT_MAPPING WILL NOT BE GENERATED.
	 * @param setCds 
	 * @param facts 
	 * @throws Exception 
	 */
	private static void execute(Collection<ObservationFact> facts, Collection<ConceptDimension> setCds) throws Exception {
		if(DO_CONCEPT_CD_SEQUENCE) {

			sequenceConcepts(facts, setCds);

		}
	}
	
	private static void sequenceConcepts(Collection<ObservationFact> facts, Collection<ConceptDimension> setCds) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		Map<String,String> conceptMap = new HashMap<String,String>();
		
		//try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
		//	CsvToBean<ConceptDimension> csvToBean = 
		//			Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
		setCds.forEach(concept ->{
				
			conceptMap.put(concept.getConceptCd(), String.valueOf(CONCEPT_CD_STARTING_SEQ));

			concept.setConceptCd(String.valueOf(CONCEPT_CD_STARTING_SEQ));
			
			CONCEPT_CD_STARTING_SEQ++;
		});
			
		
			
		facts.stream().forEach(fact -> {
			if(!conceptMap.containsKey(fact.getConceptCd())) {
				if(!fact.getConceptCd().equalsIgnoreCase("SECURITY")) 
					System.err.println("Concept ( " + fact.getConceptCd() + " )  does not exist in concpept dimension");
				return;
		 	}
			String conceptCd = conceptMap.get(fact.getConceptCd());
			
			fact.setConceptCd(conceptCd);
		});
		//}

	}

	private static void sequencePatients(Set<ObservationFact> facts) throws Exception {
		// LOAD ALL POSSIBLE PATIENT NUMS INTO A MAP<STRING,STRING> 
		Map<String,String> patientMap = new HashMap<String,String>();
		List<PatientDimension> patients = new ArrayList<PatientDimension>();
		List<PatientMapping> patientMappings = new ArrayList<PatientMapping>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "PatientMapping.csv"))){
			
			CsvToBean<PatientMapping> csvToBean = 
					Utils.readCsvToBean(PatientMapping.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
			patientMappings = csvToBean.parse();
			
			for(PatientMapping pm: patientMappings) {
				patientMap.put(pm.getPatientIde(), pm.getPatientNum());
			}
		}
		
		//try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){
			
		//	CsvToBean<ObservationFact> csvToBean = 
		//			Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);

			
		//	facts = csvToBean.parse();
			
		for(ObservationFact fact: facts) {
			if(!patientMap.containsKey(fact.getPatientNum())) {
				System.err.println("Patient Num ( " + fact.getPatientNum() + " )  does not exist in patient dimension");
				continue;
			}
			String patientNum = patientMap.get(fact.getPatientNum());
			
			fact.setPatientNum(patientNum);
		}
		//}
		//try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "PatientDimension.csv"))){
			
		//	Utils.writeToCsv(buffer, patients, DATA_QUOTED_STRING, DATA_SEPARATOR);

		//} 
		//try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){
			
		//	Utils.writeToCsv(buffer, facts, DATA_QUOTED_STRING, DATA_SEPARATOR);

		//} 
		//try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "PatientMapping.csv"))){
			
		//	Utils.writeToCsv(buffer, patientMapppings, DATA_QUOTED_STRING, DATA_SEPARATOR);

		//} 
	}

}
