package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.opencsv.CSVReader;

import etl.jobs.Job;

public class ValidateAllConcepts extends Job {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		
		validateAllConcepts();
		
	}

	private static void validateAllConcepts() throws IOException {
		int dups = 0;
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "allConcepts.csv"))){
			
			CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
			
			String[] line;
			
			Map<String,String> map = new HashMap<String,String>();
			
			while((line = reader.readNext()) != null) {
				
				String patientNum = line[0];
				
				String rootNode = line[1];
				
				rootNode = rootNode.substring(1);
				
				rootNode = rootNode.replaceAll("\\\\.*", "");
				
				if(map.containsKey(patientNum)) {
					String r = map.get(patientNum);
					if(r.equals(rootNode)) {
						continue;
					} else {
						System.err.println("DUPLICATE PATIENT HAS BEEN FOUND!!");
						System.err.println("conflicting patients are: ");
						System.err.println(r + ":" + patientNum);
						System.err.println(rootNode+ ":" + patientNum);
						dups++;
					}
				}
				
			}
			
		}
		System.out.println("Duplicates found: " + dups);
	}	
	
}
