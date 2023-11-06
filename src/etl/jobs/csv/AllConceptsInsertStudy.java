package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import etl.jobs.Job;

public class AllConceptsInsertStudy extends Job {

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
		List<String> conceptsToInsert = getConceptsToInsert();
		
		insertData(conceptsToInsert);
		
		
	}

	private static void insertData(List<String> conceptsToInsert) throws IOException {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("./hpds/allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			Boolean conceptsInserted = false;
			
			writer.write("patientNum,conceptpath,nvalnum,tvalchar,date\n");
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./hpds/newAc.csv"))) {
				
				String line;
				
				
				while((line = buffer.readLine()) != null ) {
					if(!conceptsInserted) {
						
						String[] arr = line.split(",");
						if(arr.length < 2) continue;
						if(arr[1].replaceAll("\"","").startsWith("\\Cardiovascular") ) {
							System.out.println("inserting data");
							for(String concept: conceptsToInsert) {
								writer.write(concept + '\n');
							}
							conceptsInserted = true;
						}
						
					}
					writer.write(line + '\n');
				}
				
				//return list;
			}
			
		}
		
	}

	private static List<String> getConceptsToInsert() throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./data/ORCHID_allConcepts.csv"))) {
			List<String> list = new ArrayList<>();
			
			String line;
			while((line = buffer.readLine()) != null ) {
				
				list.add(line);
				
			}
			
			return list;
		}
	}

}
