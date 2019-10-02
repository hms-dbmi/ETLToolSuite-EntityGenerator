package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.job.entity.Mapping;
import etl.job.entity.hpds.AllConcepts;

public class GenerateAllConcepts extends Job{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3961384017483551769L;
	
	private static Integer PATIENT_COL = 0;

	public static void main(String[] args) {
		
		try {
			setVariables(args, buildProperties(args));
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
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){

			mappings.forEach(mapping -> {
				
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
					
					if(SKIP_HEADERS)
						
						try {
							
							csvreader.readNext();
							
						} catch (IOException e) {
	
							System.err.println(e);
						
						}
					
					Iterator<String[]> iter = csvreader.iterator();
					
					String[] line;
					
						while((line = iter.next()) != null) {
						
							if(line.length - 1 >= column && line.length - 1 >= PATIENT_COL) {
								
								if(line[column].trim().isEmpty()) continue;
								
								AllConcepts allConcept = generateAllConcepts(mapping,line,column);
								
								if(allConcept.isValid()) {
									
									writer.write(allConcept.toCSV());
									
									writer.flush();
									
								}
								
							} else {
								
								System.err.println(line + " does not contain either/or value column " + column + " or patient column " + PATIENT_COL + " in file " + fileName);
								
							}
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

	}

	private static AllConcepts generateAllConcepts(Mapping mapping, String[] line, Integer column) {
		
		if(mapping.getDataType().equalsIgnoreCase("TEXT")) {
			
			AllConcepts allConcept = new AllConcepts();
			
			allConcept.setConceptPath(mapping.getRootNode() + line[column].trim().replaceAll("\"", "'") + '\\');
			
			allConcept.setPatientNum(line[PATIENT_COL]);
			
			allConcept.setNvalNum("");
			
			allConcept.setTvalChar(line[column].trim().replaceAll("\"", "'"));
			
			return allConcept;
			
		} else if(mapping.getDataType().equalsIgnoreCase("NUMERIC")) {
			
			AllConcepts allConcept = new AllConcepts();
			
			if(NumberUtils.isParsable(line[column].trim())){
			
				allConcept.setConceptPath(mapping.getRootNode() + line[column].trim());
				
				allConcept.setPatientNum(line[PATIENT_COL]);
				
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

}
