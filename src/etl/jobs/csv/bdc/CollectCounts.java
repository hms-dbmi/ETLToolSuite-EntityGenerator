package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.opencsv.CSVReader;

import etl.jobs.Job;

public class CollectCounts extends Job {

	private static Map<String,TreeSet<String>> patientSet = new HashMap<>();
	
	private static Map<String,TreeSet<String>> conceptSet = new HashMap<String, TreeSet<String>>();
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
						
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}	

		try {
			execute();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				if(line[1].contains("µ_consents")) {
					if(patientSet.containsKey(line[3])) {
						patientSet.get(line[3]).add(line[0]);
					}
				}
			}
	
		}
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "allConcepts.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				
				
			}
	
		}
	}

}
