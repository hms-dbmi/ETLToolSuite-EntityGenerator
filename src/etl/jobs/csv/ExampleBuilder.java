package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.jobs.Job;

public class ExampleBuilder extends Job {

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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void execute() throws IOException {
		Map<String,Integer> newPatientNums = new HashMap<>();
		
		Integer patientId = 1;
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "allConcepts.csv"))) {
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "allConcepts.csv" ), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
						.withSeparator(DATA_SEPARATOR)
						.withQuoteChar(DATA_QUOTED_STRING);
					
				RFC4180Parser parser = parserbuilder.build();

				CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
						.withCSVParser(parser);
				
				CSVReader csvreader = builder.build();
				
				Set<String> facts = new HashSet<>();
				
				Set<String> patients = new HashSet<>();

				String currentConcept = new String();
				
				
				String[] cells;
				while((cells = csvreader.readNext()) != null) {
					
					if(!NumberUtils.isCreatable(cells[0])) continue;
					
					Integer patientNum = new Integer(cells[0]);
					
					if(currentConcept.equals(cells[1])) {
						
						if(patients.size() >= 10) { // 10 patients already for this concept
							continue;
							
						}
						
					} else {
						
						patients = new HashSet<>();
						
						currentConcept = cells[1];
												
					}
					
					if(newPatientNums.containsKey(patientNum.toString())) {
						
						patientNum = newPatientNums.get(patientNum.toString());
						
					} else {
						
						newPatientNums.put(patientNum.toString(), patientId);
						patientNum = patientId;
						patientId++;
						
					}
					
					cells[0] = patientNum.toString();
					
					cells[4] = "0";
					
					patients.add(cells[0]);
					writer.write(toCsv(cells));
					
					
					
				}
			
			}
		}
		
		
	}

}
