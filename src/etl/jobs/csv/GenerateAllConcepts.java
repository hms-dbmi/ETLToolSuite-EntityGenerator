package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.job.entity.hpds.AllConcepts;
import etl.jobs.Job;
import etl.jobs.jobproperties.JobProperties;
import etl.jobs.mappings.Mapping;
import etl.jobs.mappings.PatientMapping;

/**
 * CSV Version to generate allConcepts file
 * 
 *
 */
public class GenerateAllConcepts extends Job {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3961384017483551769L;

	private static final char OLD_SEPARATOR = 'µ';

	private static final boolean BUILD_VAR_REPORTS = false;

	private static boolean USE_PATIENT_MAPPING = true;

	
	private static Set<String> MAPPINGS_WITH_BAD_DATA_TYPES = new HashSet<>();
	
	// Static map to see if patient has been created
	private static Map<String,Integer> SEQUENCE_MAP = new HashMap<String,Integer>();
	
	private static Map<String,VariableAnalysis> VARIABLE_ANALYSIS = new TreeMap<>();
	
	public static class VariableAnalysis {
		
		private static final String ANALYSIS_DIR = "./reports/";
		public String fileName;
		public int colIdx;
		public String conceptPath;
		public String mappingDataType;
		public int totalFileRecordCount;  // valid + invalid variables + null/empty values
		public int totalVariableCount;  // valid + invalid variables 
		public int totalValidVariableCount;  // valid variables only
		public int totalBadNumericCount; // invalid variables duxe to bad data type
		public int totalEmptyOrNullValueCount; // 
		public int totalPossibleNumericVariablesAsTextCount; // Text value that pass numeric isCreateable.
		
		public BigDecimal badNumericPercent;
		public BigDecimal possibleNumericVariablesAsTextPercentage;

		public Map<String,Integer> badNumericalVariables = new HashMap<>();
		public Map<String,Integer> possibleNumericVariablesAsText = new HashMap<>();
		
		public VariableAnalysis(Mapping mapping) {
		
			this.fileName = mapping.getKey().split(":")[0];
			this.colIdx = new Integer(mapping.getKey().split(":")[1]);
			this.mappingDataType = mapping.getDataType();
			this.conceptPath = mapping.getRootNode();
			this.totalFileRecordCount = 0;
			this.totalVariableCount = 0;
			this.totalValidVariableCount = 0;
			this.totalBadNumericCount = 0;
			this.totalEmptyOrNullValueCount = 0;
			this.totalPossibleNumericVariablesAsTextCount = 0;
			
		}

		public static void writeReports() throws IOException {
			writeReportOverview();
			writeVariableReports();
		}
		private static void writeVariableReports() throws IOException {
			for(Entry<String,VariableAnalysis> VAEntry: VARIABLE_ANALYSIS.entrySet()) {

				try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(ANALYSIS_DIR + TRIAL_ID + "_" + VAEntry.getKey() + "_VariableReport.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
					
					VariableAnalysis va = VAEntry.getValue();
					
					if(va.mappingDataType.equalsIgnoreCase("TEXT")) {
						String[] headers = { "FILENAME", "COLUMN_INDEX", "CONCEPT_PATH", "MAPPING_DATA_TYPE", "POSSIBLE_NUMERIC_VALUE", "FREQUENCY" };
						
						buffer.write(toCsv(headers));
						for(Entry<String,Integer> pnv: va.possibleNumericVariablesAsText.entrySet()) {
							String[] lineToWrite = new String[headers.length];
							lineToWrite[0] = va.fileName;
							lineToWrite[1] = new Integer(va.colIdx).toString();
							lineToWrite[2] = va.conceptPath;
							lineToWrite[3] = va.mappingDataType;
							lineToWrite[4] = pnv.getKey();
							lineToWrite[5] = pnv.getValue().toString();
							
							buffer.write(toCsv(lineToWrite));
						}
					}
					if(va.mappingDataType.equalsIgnoreCase("NUMERIC")) {
						String[] headers = { "FILENAME", "COLUMN_INDEX", "CONCEPT_PATH", "MAPPING_DATA_TYPE", "INVALID_NUMERIC_VALUE", "FREQUENCY" };
						
						buffer.write(toCsv(headers));
						for(Entry<String,Integer> bnv: va.badNumericalVariables.entrySet()) {
							String[] lineToWrite = new String[headers.length];
							lineToWrite[0] = va.fileName;
							lineToWrite[1] = new Integer(va.colIdx).toString();
							lineToWrite[2] = va.conceptPath;
							lineToWrite[3] = va.mappingDataType;
							lineToWrite[4] = bnv.getKey();
							lineToWrite[5] = bnv.getValue().toString();
							
							buffer.write(toCsv(lineToWrite));
						}
					}
					
				}	
			}
			
		}

		private static void writeReportOverview() throws IOException {
			try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(ANALYSIS_DIR + TRIAL_ID + "_VariableAnalysisOverview.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
				
				String[] headers = { "FILENAME", "COLUMN_INDEX", "CONCEPT_PATH", "MAPPING_DATA_TYPE", "TOTAL_RECORDS_IN_FILE",
						"TOTAL_VARIABLE_COUNT", "TOTAL_VALID_VARIABLE_COUNT", "BAD_NUMERIC_COUNT",
						"EMPTY_OR_NULL_COUNT", "POSSIBLE_NUMERIC_VARIABLES_AS_TEXT" };
				
				buffer.write(toCsv(headers));
				
				for(Entry<String,VariableAnalysis> VAEntry: VARIABLE_ANALYSIS.entrySet()) {
					VariableAnalysis va = VAEntry.getValue();
					String[] lineToWrite = new String[headers.length];
					lineToWrite[0] = va.fileName;
					lineToWrite[1] = new Integer(va.colIdx).toString();
					lineToWrite[2] = va.conceptPath;
					lineToWrite[3] = va.mappingDataType;
					lineToWrite[4] = new Integer(va.totalFileRecordCount).toString();
					lineToWrite[5] = new Integer(va.totalVariableCount).toString();
					lineToWrite[6] = new Integer(va.totalValidVariableCount).toString();
					lineToWrite[7] = new Integer(va.totalBadNumericCount).toString();
					lineToWrite[8] = new Integer(va.totalEmptyOrNullValueCount).toString();
					lineToWrite[9] = new Integer(va.totalPossibleNumericVariablesAsTextCount).toString();
					
					buffer.write(toCsv(lineToWrite));
				}
				
			}
			
		}
		
	}

	
	public static void main(String[] args) {
		
		try {

			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			e.printStackTrace();
			
		}	
		
		try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			
		}
		
	}

	private static void execute() throws IOException {
		Mapping.PATH_SEPARATOR = PATH_SEPARATOR;
		
		List<Mapping> mappings = Mapping.generateMappingListForHPDS(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		List<PatientMapping> patientMappings = new ArrayList<>();
		
		// populate seq map with current mappings
		//if(USE_PATIENT_MAPPING) buildSeqMap();
		
		if(USE_PATIENT_MAPPING ) {
			
			patientMappings = PatientMapping.readPatientMappingFile(PATIENT_MAPPING_FILE);
			
			SEQUENCE_MAP = PatientMapping.buildSeqMap(patientMappings);
			
			if(!SEQUENCE_MAP.isEmpty()) {
				PATIENT_NUM_STARTING_SEQ = Collections.max(SEQUENCE_MAP.values()) + 1;
			}
			
		}
		
		doGenerateAllConcepts(patientMappings, mappings);
		writeBadMappings();	
		if(BUILD_VAR_REPORTS) {
			VariableAnalysis.writeReports();
		}
	}

	private static void writeBadMappings() throws IOException {
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_BadDataTypes.txt"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			for(String line: MAPPINGS_WITH_BAD_DATA_TYPES) {
				buffer.write(line + '\n');
			}
		}
		
	}

	private static void doGenerateAllConcepts(List<PatientMapping> patientMappings, List<Mapping> mappings) {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			mappings.forEach(mapping -> {
				
				// validations
				if(mapping.getKey().split(":").length != 2) return;
				if(mapping.getRootNode().contains(String.valueOf(OLD_SEPARATOR))) {
					
					mapping.setRootNode(mapping.getRootNode().replace(OLD_SEPARATOR,PATH_SEPARATOR.charAt(0)));
					
				};
				
				String[] options = mapping.getOptions().split(":");
	
				for(String option: options) {
					
					if(option.split("=").length != 2) continue;
					
					String optionkey = option.split("=")[0];
					
					optionkey = optionkey.replace(String.valueOf(MAPPING_QUOTED_STRING), "");
					
					if(optionkey.equalsIgnoreCase("patientcol")) {
						PATIENT_COL = Integer.valueOf(option.split("=")[1]);
					} 
				}
	 
				String fileName = mapping.getKey().split(":")[0];
								
				Integer column = new Integer(mapping.getKey().split(":")[1]);
				
				if(!Files.exists(Paths.get(DATA_DIR + fileName))) {
					
					System.err.println(DATA_DIR + fileName + " does not exist.");
					
					return;
					
				} 
				
				try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + fileName))){
	
					RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
							.withSeparator(DATA_SEPARATOR)
							.withQuoteChar(DATA_QUOTED_STRING);
						
					RFC4180Parser parser = parserbuilder.build();
	
					CSVReaderBuilder builder = new CSVReaderBuilder(reader)
							.withCSVParser(parser);
					
					CSVReader csvreader = builder.build();
					String[] headers = null;
					if(SKIP_HEADERS) {
						
						try {
							
							headers = csvreader.readNext();
							
						} catch (IOException e) {
	
							System.err.println(e);
						    e.printStackTrace();
						}
					}
										
					String[] line;
						// hash set to store  unique concepts
					Set<AllConcepts> records = new TreeSet<>( new Comparator<AllConcepts>() {
						@Override
						public int compare(AllConcepts o1, AllConcepts o2) {
							if(o1 == null || o2 == null) return -1;
							
							int conceptpath = o1.getConceptPath().compareTo(o2.getConceptPath());
							
							if(conceptpath != 0) {
								return conceptpath;
							}
							
							int patientNum = o1.getPatientNum().compareTo(o2.getPatientNum());
							
							if(patientNum != 0) {
								return patientNum;
							}
							
							int tvalChar = o1.getTvalChar().compareTo(o2.getTvalChar());
							
							if(tvalChar != 0) {
								return tvalChar;
							}
							
							return(o1.getNvalNum().compareTo(o2.getNvalNum()));
							
													
						}
						
					} ); 
					
					while((line = csvreader.readNext()) != null) {
						if(headers == null) headers = line; // if headers is null set headers to first row of data will be used to check for malformed rows.
						if(headers.length != line.length) continue;  // skip malformed rows  
						if(BUILD_VAR_REPORTS) {
							analyzeVariable(mapping, line);
						}
						if(line.length == 0) continue;
						// hotfix for dbgap
						if(line[0].toLowerCase().contains("dbgap")) continue;
						if(line.length - 1 >= column && line.length - 1 >= PATIENT_COL) {
							if(line[0].trim().isEmpty()) continue;
							//if(line[column].trim().isEmpty()) continue;
							if(line[0].charAt(0) == '#') continue;
							AllConcepts allConcept = generateAllConcepts(mapping,line,column);
							
							if(allConcept.isValid()) {
								
								records.add(allConcept);
								
							}
							
						} else {
							
							System.err.println(Arrays.asList(line) + " does not contain either/or value column " + column + " or patient column " + PATIENT_COL + " in file " + fileName);
							
						}
					}	
					
					for(AllConcepts ac: records) {
						
						writer.write(ac.toCSV());
						
					}
					
					writer.flush();

				} catch (IOException e1) {
					
					System.err.println(e1);
					e1.printStackTrace();
				}
				
			});
			
			writer.close();
			
		} catch (IOException e2) {

			System.err.println(e2);
			
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_PatientMapping.v2.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			String[] stringToWrite = new String[3];
			
			for(Entry<String,Integer> entry: SEQUENCE_MAP.entrySet()) {
				
				stringToWrite[0] = entry.getKey();

				stringToWrite[1] = TRIAL_ID;
				
				stringToWrite[2] = entry.getValue().toString();
				
				writer.write(toCsv(stringToWrite));
			}
			
			writer.flush();
			
			writer.close();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}

	private static AllConcepts generateAllConcepts(Mapping mapping, String[] line, Integer column) throws IOException {
		
		AllConcepts allConcept = new AllConcepts();
		
		if(mapping.getDataType().equalsIgnoreCase("TEXT")) {
			
			
			allConcept.setConceptPath(mapping.getRootNode());
						
			if(DO_PATIENT_NUM_SEQUENCE) {
				allConcept.setPatientNum(sequencePatient(line[PATIENT_COL]));
			} else {
				if(NumberUtils.isCreatable(line[PATIENT_COL])) {
					allConcept.setPatientNum(new Integer(line[PATIENT_COL]));
				}
			}
			
			allConcept.setNvalNum("");
			
			allConcept.setTvalChar(line[column].trim().replaceAll("\"", "'"));
			
		} else if(mapping.getDataType().equalsIgnoreCase("NUMERIC")) {
						
			if(NumberUtils.isCreatable(line[column].trim())){
			
				allConcept.setConceptPath(mapping.getRootNode());
				
				if(DO_PATIENT_NUM_SEQUENCE) {
					allConcept.setPatientNum(sequencePatient(line[PATIENT_COL]));
				} else {
					if(NumberUtils.isCreatable(line[PATIENT_COL])) {
						allConcept.setPatientNum(new Integer(line[PATIENT_COL]));
					}
				}
								
				allConcept.setNvalNum(line[column].trim());
				
				allConcept.setTvalChar("");
				
				return allConcept;
				
			} else {
				
				//System.err.println("Value for record " + line[column].trim() + " in file " + mapping.getKey() + " is not numeric.");
				
				MAPPINGS_WITH_BAD_DATA_TYPES.add(mapping.getKey());
				
				
			}
		} else {
			System.err.println("Invalid data type for " + mapping.getKey() + " " + mapping.getDataType());
		
		}
		
		//analyzeVariable(mapping, line);
		
		return allConcept;

	}

	private static void analyzeVariable(Mapping mapping, String[] line) {
		
		VariableAnalysis va;
		//Map<String,Integer> invalidVariables;
		
		if(VARIABLE_ANALYSIS.containsKey(mapping.getKey())) {
			va = VARIABLE_ANALYSIS.get(mapping.getKey());
			//invalidVariables = VARIABLE_ANALYSIS.get(mapping.getKey()).invalidVariables;
		} else {
			va = new VariableAnalysis(mapping);
		}

		// iterate file record count
		va.totalFileRecordCount++;
		
		String value = (line.length - 1 ) >= va.colIdx ? line[va.colIdx]: null;
		
		// Logic for null and empty values
		if(value == null || value.trim().isEmpty()) {
			// iterate empty or null value count
			va.totalEmptyOrNullValueCount++;
		} else {
			// Increment total variable count as value is not null or empty
			va.totalVariableCount++;
			
			// Text Data type analysis
			if(mapping.getDataType().equalsIgnoreCase("TEXT")) {
			// text is presumed to be valid unless it is empty or null
				va.totalValidVariableCount++;
			// Can text be numeric?
				if(NumberUtils.isCreatable(value)) {
					
					va.totalPossibleNumericVariablesAsTextCount++;
					
					va.possibleNumericVariablesAsText.merge(value, 1, Integer::sum);
	
				} 
			}
			// Numeric Data type analysis
			else if(mapping.getDataType().equalsIgnoreCase("NUMERIC")) {
				// is a Createable Java Numerical type?
				if(!NumberUtils.isCreatable(value)) {
					// if create it is valid
				    va.totalBadNumericCount++;
				    
				    va.badNumericalVariables.merge(value, 1, Integer::sum);
					
				}
			}	
		}
		VARIABLE_ANALYSIS.put(mapping.getKey(), va);
		
	}

	private static Integer sequencePatient(String patientNum) throws IOException {
		if(SEQUENCE_MAP.containsKey(patientNum)) {
			return SEQUENCE_MAP.get(patientNum);
		} else {
			System.out.println("Patient Mapping missing for: " + patientNum );
			return -1;
		}
	}

	protected static void setLocalVariables(String[] args, JobProperties properties) throws Exception {
		if(properties != null) {
			if(properties.contains("usepatientmapping")) {
				if(new String(StringUtils.substring(properties.getProperty("usepatientmapping"),0,1)).equalsIgnoreCase("Y")){
					USE_PATIENT_MAPPING = true;
				}
			}
			if(properties.contains("patientcol")) {
				
				PATIENT_COL = new Integer(properties.get("patientcol").toString());
				
			}
		}
	}
		
}
