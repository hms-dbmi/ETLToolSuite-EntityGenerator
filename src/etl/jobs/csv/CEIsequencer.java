package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;
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

	/**
	 * Main method to execute as a stand alone process.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			if(DO_SEQUENCING) execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(e);
			e.printStackTrace();
		}
	}
	/**
	 * Main method to execute as a stand alone process.
	 * @param args
	 */
	public static void main(String[] args, JobProperties jobProperties) {
		try {
			setVariables(args, jobProperties);
		} catch (Exception e) {
			System.err.println(e);
		}
		
		try {
			if(DO_SEQUENCING) execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		}
	}
	/**
	 * Main method to run if facts and concepts exist in memory already
	 * @param args
	 * @param facts
	 * @param setCds
	 * @param buildProperties
	 */
	public static void main(String[] args, Collection<ObservationFact> facts, Collection<ConceptDimension> setCds, JobProperties buildProperties ) {
		try {
			setVariables(args, buildProperties);
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			if(DO_SEQUENCING) execute(facts, setCds);
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}
	/**
	 *  ConceptCD SEQUENCED IN OBSERVATIONFACT.CSV AND PATIENTDIMENSION.CSV
	 *  
	 * @param setCds 
	 * @param facts 
	 * @throws Exception 
	 */
	private static void execute() throws Exception {
		if(DO_CONCEPT_CD_SEQUENCE) {
			sequenceConcepts();
		}
		if(DO_PATIENT_NUM_SEQUENCE) {
			sequencePatients();
		}
	}
	/**
	 * This is built to be run by the Entity Generator job.
	 * It takes facts and concepts that exist in memory and processes them
	 * instead of reading from disk.s
	 * 
	 * @param facts
	 * @param setCds
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 * @throws IOException
	 */
	private static void execute(Collection<ObservationFact> facts, Collection<ConceptDimension> setCds) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException, IOException {
		if(DO_CONCEPT_CD_SEQUENCE) {
			sequenceConcepts(facts,setCds);
		}
		if(DO_PATIENT_NUM_SEQUENCE) {
			sequencePatients(facts);
		}
		
	}
	/**
	 * Reads generated patient mapping file and will sequence the nums in the 
	 * generated facts
	 * @param facts 
	 * @throws IOException 
	 * @throws CsvRequiredFieldEmptyException 
	 * @throws CsvDataTypeMismatchException 
	 */
	private static void sequencePatients() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "PatientMapping.csv"))){
		
			CsvToBean<PatientMapping> csvToBean = 
					Utils.readCsvToBean(PatientMapping.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);		
			
			Map<String, String> idlookup = csvToBean.parse().stream().collect(
					Collectors.toMap(
							PatientMapping::getPatientIde,
							PatientMapping::getPatientNum)
					);
			
			try(BufferedReader bufferfacts = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){
			
				CsvToBean<ObservationFact> facts = 
						Utils.readCsvToBean(ObservationFact.class, bufferfacts, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);		
				
				List<ObservationFact> factsl = facts.parse();
				
				factsl.stream().forEach(fact ->{
					if(idlookup.containsKey(fact.getPatientNum())) {
						fact.setPatientNum(idlookup.get(fact.getPatientNum()));
					} 
				});
				bufferfacts.close();			
				try(BufferedWriter buffer2 = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){
					
					Utils.writeToCsv(buffer2, factsl, DATA_QUOTED_STRING, DATA_SEPARATOR);
			
				} 
			}
		}
	}
	/**
	 * Reads generated patient mapping file and will sequence the nums in the 
	 * generated facts
	 * @param facts 
	 * @throws IOException 
	 */
	public static void sequencePatients(Collection<ObservationFact> facts) throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "PatientMapping.csv"))){
		
			CsvToBean<PatientMapping> csvToBean = 
					Utils.readCsvToBean(PatientMapping.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);		
			
			Map<String, String> idlookup = csvToBean.parse().stream().collect(
					Collectors.toMap(PatientMapping::getPatientIde, 
							PatientMapping::getPatientNum)
					);
			
			facts.stream().forEach(fact ->{
				if(idlookup.containsKey(fact.getPatientNum())) {
					fact.setPatientNum(idlookup.get(fact.getPatientNum()));
				} else {
					System.err.println("Patient Does not exist: " + fact.getPatientNum());
				}
			});
		}
	}
	/**
	 * This method requires that concepts and facts already be generated.
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static void sequenceConcepts() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		Map<String,String> conceptMap = new HashMap<String,String>();
		List<ConceptDimension> concepts = new ArrayList<ConceptDimension>();
		
		if(!Files.exists(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))) {
			throw new IOException("Concept Dimension file does not exist. Run Concept Generator.");
		}
		
		if(!Files.exists(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))) {
			throw new IOException("Observation Fact file does not exist. Run Fact Generator.");
		}
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
			CsvToBean<ConceptDimension> csvToBean = 
					Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);
			
			concepts = csvToBean.parse();
			
			concepts.forEach(concept ->{
				
				conceptMap.put(concept.getConceptCd(), String.valueOf(CONCEPT_CD_STARTING_SEQ));
	
				concept.setConceptCd(String.valueOf(CONCEPT_CD_STARTING_SEQ));
				
				CONCEPT_CD_STARTING_SEQ++;
			});
			
		}
			
		List<ObservationFact> facts = new ArrayList<ObservationFact>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){
			
			CsvToBean<ObservationFact> csvToBean = 
					Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false );
	
			
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
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
			Utils.writeToCsv(buffer, concepts, DATA_QUOTED_STRING, DATA_SEPARATOR);
	
		} 
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){
			
			Utils.writeToCsv(buffer, facts, DATA_QUOTED_STRING, DATA_SEPARATOR);
	
		} 
	
	}
	/**
	 * This method can be used if facts and concepts are already stored in memory.
	 * @param facts
	 * @param setCds
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	public static void sequenceConcepts(Collection<ObservationFact> facts, Collection<ConceptDimension> setCds) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		Map<String,String> conceptMap = new HashMap<String,String>();

		setCds.forEach(concept ->{
				
			conceptMap.put(concept.getConceptCd(), String.valueOf(CONCEPT_CD_STARTING_SEQ));

			concept.setConceptCd(String.valueOf(CONCEPT_CD_STARTING_SEQ));
			
			CONCEPT_CD_STARTING_SEQ++;
		});
			
		facts.stream().forEach(fact -> {
			if(!conceptMap.containsKey(fact.getConceptCd())) {
				if(!fact.getConceptCd().equalsIgnoreCase("SECURITY")) 
					System.err.println("Concept ( " + fact.getConceptCd() + " )  does not exist in concept dimension");
				return;
		 	}
			String conceptCd = conceptMap.get(fact.getConceptCd());
			
			fact.setConceptCd(conceptCd);
		});
	}
}
