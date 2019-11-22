package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.job.entity.Mapping;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.jobproperties.JobProperties;

public class GenerateAllConcepts extends Job{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3961384017483551769L;

	private static final char OLD_SEPARATOR = 'µ';

	private static boolean USE_PATIENT_MAPPING = false;
	
	private static Integer PATIENT_COL = 0;
	
	
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

		List<Mapping> mappings = Mapping.generateMappingListForHPDS(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		doGenerateAllConcepts(mappings);
			
	}

	private static void doGenerateAllConcepts(List<Mapping> mappings) {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			mappings.forEach(mapping -> {
				
				mapping.setRootNode(mapping.getRootNode().replace(OLD_SEPARATOR,PATH_SEPARATOR.charAt(0)));
				
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
					
					Iterator<String[]> iter = csvreader.iterator();
					
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
							
							return o1.getPatientNum().compareTo(o2.getPatientNum());
													
						}
						
					} ); 
					
					while((line = iter.next()) != null) {
					
						if(line.length - 1 >= column && line.length - 1 >= PATIENT_COL) {
							
							if(line[column].trim().isEmpty()) continue;
							
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
					
				}
				
			});
			
			writer.close();
			
		} catch (IOException e2) {

			System.err.println(e2);
			
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_PatientMapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			String[] stringToWrite = new String[3];
			
			for(Entry<String,Integer> entry: PATIENT_SEQED_MAP.entrySet()) {
				
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

	private static char[] toCsv(String[] strings) {
		StringBuilder sb = new StringBuilder();
		
		int x = strings.length;
		
		for(String str: strings) {
			sb.append('"');
			sb.append(str);
			sb.append('"');
			if(x != 1 ) {
				sb.append(',');
			}
			x--;
		}
		sb.append('\n');
		return sb.toString().toCharArray();
	}

	private static AllConcepts generateAllConcepts(Mapping mapping, String[] line, Integer column) throws IOException {
		
		if(mapping.getDataType().equalsIgnoreCase("TEXT")) {
			
			AllConcepts allConcept = new AllConcepts();
			
			allConcept.setConceptPath(mapping.getRootNode() + line[column].trim().replaceAll("\"", "'") + PATH_SEPARATOR);
			if(DO_PATIENT_NUM_SEQUENCE) {
				allConcept.setPatientNum(sequencePatient(line[PATIENT_COL]));
			} else {
				allConcept.setPatientNum(new Integer(line[PATIENT_COL]));
			}
			allConcept.setNvalNum("");
			
			allConcept.setTvalChar(line[column].trim().replaceAll("\"", "'"));
			
			return allConcept;
			
		} else if(mapping.getDataType().equalsIgnoreCase("NUMERIC")) {
			
			AllConcepts allConcept = new AllConcepts();
			
			if(NumberUtils.isParsable(line[column].trim())){
			
				allConcept.setConceptPath(mapping.getRootNode());
				
				if(DO_PATIENT_NUM_SEQUENCE) {
					allConcept.setPatientNum(sequencePatient(line[PATIENT_COL]));
				} else {
					allConcept.setPatientNum(new Integer(line[PATIENT_COL]));
				}
				
				allConcept.setNvalNum(line[column].trim());
				
				allConcept.setTvalChar("E");
				
				return allConcept;
				
			} else {
				
				System.err.println("Value for record " + line + " in file " + mapping.getKey() + " is not numeric.");
				
				return new AllConcepts();
				
			}
		} else {
			System.err.println("Invalid data type for " + mapping.getKey() + " " + mapping.getDataType());
			return new AllConcepts();
		
		}
	}
	
	// Static map to see if patient has been created
	private static Map<String,Integer> PATIENT_SEQED_MAP = new HashedMap<String,Integer>();
	
	private static Integer sequencePatient(String patientNum) throws IOException {
		if(PATIENT_SEQED_MAP.containsKey(patientNum)) {
			return PATIENT_SEQED_MAP.get(patientNum);
		} else {
			PATIENT_SEQED_MAP.put(patientNum, PATIENT_NUM_STARTING_SEQ);
			PATIENT_NUM_STARTING_SEQ++;
			return PATIENT_NUM_STARTING_SEQ - 1;
		}
	}

	protected static void setLocalVariables(String[] args, JobProperties properties) throws Exception {
		if(properties != null) {
			if(properties.contains("usepatientmapping")) {
				if(new String(StringUtils.substring(properties.getProperty("usepatientmapping"),0,1)).equalsIgnoreCase("Y")){
					USE_PATIENT_MAPPING = true;
				}
			}
		}
	}
		
}
