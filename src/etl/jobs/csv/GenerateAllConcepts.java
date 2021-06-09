package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
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
public class GenerateAllConcepts extends Job{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3961384017483551769L;

	private static final char OLD_SEPARATOR = 'µ';

	private static boolean USE_PATIENT_MAPPING = false;
	
	private static Integer PATIENT_COL = 0;
	
	private static Set<String> MAPPINGS_WITH_BAD_DATA_TYPES = new HashSet<>();
	
	// Static map to see if patient has been created
	private static Map<String,Integer> SEQUENCE_MAP = new HashMap<String,Integer>();
		
	public static void main(String[] args) {
		
		try {

			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
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
					
					if(SKIP_HEADERS) {
						
						try {
							
							csvreader.readNext();
							
						} catch (IOException e) {
	
							System.err.println(e);
						
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
						if(line.length == 0) continue;
						// hotfix for dbgap
						if(line[0].toLowerCase().contains("dbgap")) continue;
						if(line.length - 1 >= column && line.length - 1 >= PATIENT_COL) {
							if(line[0].trim().isEmpty()) continue;
							if(line[column].trim().isEmpty()) continue;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static AllConcepts generateAllConcepts(Mapping mapping, String[] line, Integer column) throws IOException {
		
		if(mapping.getDataType().equalsIgnoreCase("TEXT")) {
			
			AllConcepts allConcept = new AllConcepts();
			
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

			return allConcept;
			
		} else if(mapping.getDataType().equalsIgnoreCase("NUMERIC")) {
			
			AllConcepts allConcept = new AllConcepts();
			
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
				
				System.err.println("Value for record " + line[column].trim() + " in file " + mapping.getKey() + " is not numeric.");
				
				MAPPINGS_WITH_BAD_DATA_TYPES.add(mapping.getKey());
				
				return new AllConcepts();
				
			}
		} else {
			System.err.println("Invalid data type for " + mapping.getKey() + " " + mapping.getDataType());
			return new AllConcepts();
		
		}
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
