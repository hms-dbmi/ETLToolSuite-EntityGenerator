package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ConceptCounts;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.Job;
import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * This class purpose is to process a data file and generate
 * the Observation Fact entity file that can be loaded into a data store.
 * 
 */
public class FactGenerator extends Job {	

	private static Integer START_DATE_COL = null;

	private static Integer END_DATE_COL = null;
	
	private static Integer PATIENT_COL = 0;
	
	private static Integer ENCOUNTER_COL = -1;
	
	private static Integer INSTANCE_COL = -1;
	
	/**
	 * Standalone main method
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			writeFacts(execute(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException | IOException e) {
			System.err.println(e);
		}
		
	}
	/**
	 * Main method that executes subprocesses
	 * Exception handling should happen here. Unless related to streams.
	 * 
	 * @param args
	 * @param buildProperties 
	 * @return 
	 **/
	public static Set<ObservationFact> main(String[] args, JobProperties buildProperties) {
		try {
			setVariables(args, buildProperties);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			return execute();
		} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException | IOException e) {
			System.err.println(e);
			e.printStackTrace();
		}
		return null;
		
	}
	
	/**
	 * Wrapper for subprocesses
	 * @return 
	 * 
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static Set<ObservationFact> execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {

		List<Mapping> mappings = new ArrayList<Mapping>();

		mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		return doFactGenerator(mappings);

	}
	

	
	/**
	 * Generates facts based on information contained in the passed mapping records.
	 * 
	 * @param mappings
	 * @return 
	 * @throws IOException
	 */
	private static Set<ObservationFact> doFactGenerator(List<Mapping> mappings) throws IOException {
		
		String defaultDate = new java.sql.Date(new java.util.Date().getTime()).toString();
		Set<ObservationFact> facts = new HashSet<ObservationFact>();
		List<ConceptCounts> counts = new ArrayList<ConceptCounts>();
		// if text will be multiple nodes and associated patient sets
		Map<String, Set<String>> countMap = new HashedMap<String, Set<String>>();
		
		mappings.forEach(mapping -> {
			
			if(mapping.getKey().split(":").length !=2) return;
			
			String[] options = mapping.getOptions().split(":");
			
			for(String option: options) {
				
				if(option.split("=").length != 2) continue;
				
				String optionkey = option.split("=")[0];
				
				optionkey = optionkey.replace(String.valueOf(MAPPING_QUOTED_STRING), "");
				
				if(optionkey.equalsIgnoreCase("patientcol")) {
					PATIENT_COL = Integer.valueOf(option.split("=")[1]);
				} else if(optionkey.equalsIgnoreCase("startdatecol")) {
					START_DATE_COL = Integer.valueOf(option.split("=")[1]);
				} else if(optionkey.equalsIgnoreCase("enddatecol")) {
					END_DATE_COL = Integer.valueOf(option.split("=")[1]);
				}
			}
			
			String fileName = mapping.getKey().split(":")[0];
			
			Integer column = new Integer(mapping.getKey().split(":")[1]);
			
			if(!Files.exists(Paths.get(DATA_DIR + File.separatorChar + fileName))) {
				
				System.err.println(DATA_DIR + File.separatorChar + fileName + " does not exist.");
				
				return;
				
			}
			
			try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + fileName))){
				RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
						.withSeparator(DATA_SEPARATOR)
						.withQuoteChar(DATA_QUOTED_STRING);
					
				RFC4180Parser parser = parserbuilder.build();

				CSVReaderBuilder builder = new CSVReaderBuilder(reader)
						.withCSVParser(parser);
				
				CSVReader csvreader = builder.build();
				
				if(SKIP_HEADERS) csvreader.readNext();

				List<String[]> records = csvreader.readAll();
				// Dump the facts object and start a bufferwriter to the observationfact file.				
				records.forEach(record ->{

					ObservationFact obs = new ObservationFact();
					if(record[column] == null) return;
					if(record[column].trim().equalsIgnoreCase("null")) return;
					if(record.length - 1 < column) return;
					if(record[column].trim().isEmpty()) return;
					
					String conceptCd = mapping.getDataType().equalsIgnoreCase("numeric") ?
							mapping.getRootNode() :mapping.getRootNode() + record[column] + PATH_SEPARATOR; 
							
					String encounterNum = ENCOUNTER_COL == -1 ? "-1" : record[ENCOUNTER_COL];
					String startDate = START_DATE_COL == null ? defaultDate : record[START_DATE_COL];
					String endDate = END_DATE_COL == null ? defaultDate : record[END_DATE_COL];

					String instanceNum = INSTANCE_COL == -1 ? "-1" : record[INSTANCE_COL];
					String tvalChar = mapping.getDataType().equalsIgnoreCase("TEXT") ? record[column]: "E";
					String nvalNum = mapping.getDataType().equalsIgnoreCase("TEXT") ? "": record[column];
					String valTypeCd = mapping.getDataType().equalsIgnoreCase("TEXT") ? "T": "N";
					
					String patientNum = record[PATIENT_COL].isEmpty() ? null: record[PATIENT_COL];
												
					if(patientNum == null) return;
					
					obs.setPatientNum(patientNum);
					obs.setConceptCd(conceptCd);
					obs.setEncounterNum(encounterNum);
					obs.setInstanceNum(instanceNum);
					obs.setModifierCd("@");
					obs.setValtypeCd(valTypeCd);
					obs.setTvalChar(tvalChar);
					obs.setNvalNum(nvalNum);
					obs.setSourceSystemCd(TRIAL_ID);
					obs.setStartDate(startDate);
					obs.setEndDate(endDate);
					
					facts.add(obs);
					
					if(countMap.containsKey(conceptCd)) countMap.get(conceptCd).add(patientNum);
					else countMap.put(conceptCd, new HashSet<String>( Arrays.asList(patientNum)));
	
				});
			} catch (IOException e) {
				System.err.println(e);
				e.printStackTrace();
			} 			
		});	
		
		String countsProcessingFileName = PROCESSING_FOLDER + CONFIG_FILENAME.replace(".config", "").replace("config.", "") + "_ConceptCounts.csv";
				
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(countsProcessingFileName), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			StatefulBeanToCsv<ConceptCounts> writer = new StatefulBeanToCsvBuilder<ConceptCounts>(buffer)
					.withQuotechar(DATA_QUOTED_STRING)
					.withSeparator(DATA_SEPARATOR)
					.build();
			
			Set<ConceptCounts> cnts = new HashSet<ConceptCounts>();
			
			for(Entry<String, Set<String>> entry: countMap.entrySet()) {

				int x = StringUtils.countMatches(entry.getKey(), PATH_SEPARATOR);
				
				String parentConceptPath = entry.getKey().substring(0, StringUtils.ordinalIndexOf(entry.getKey(), PATH_SEPARATOR, (x - 1)) + 1);
				
				ConceptCounts cc = new ConceptCounts();
				cc.setConceptPath(entry.getKey());
				cc.setParentConceptPath(parentConceptPath);
				cc.setPatientCount(entry.getValue().size());
				
				cnts.add(cc);
			}
			
			Utils.writeToCsv(buffer, cnts, DATA_QUOTED_STRING, DATA_SEPARATOR);
			
			buffer.flush();
			buffer.close();
			
		} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
			
			System.err.println(e);
			
			e.printStackTrace();
			
		}
		
		return facts;
	}
	
	public static BufferedWriter openBufferedWriter(String fileName, StandardOpenOption... options) throws IOException {
		return Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + fileName), options);
	}
	
	public static void writeFacts(Set<ObservationFact> facts, StandardOpenOption... options) {
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(PROCESSING_FOLDER + File.separatorChar + CONFIG_FILENAME + '_' + FACT_FILENAME), options)){
			try {
				Utils.writeToCsv(buffer, facts.stream().collect(Collectors.toList()), DATA_QUOTED_STRING, DATA_SEPARATOR);
			} catch (CsvDataTypeMismatchException e) {
				e.printStackTrace();
			} catch (CsvRequiredFieldEmptyException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			System.err.println("Error writing Observation Facts");
			System.err.println(e);
			e.printStackTrace();
		} 
	}

}
