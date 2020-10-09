package src.deprecated;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.opencsv.CSVReader;

import etl.jobs.Job;

public class LookForDupConsents extends Job {

	public static void main(String[] args) {
		try {

			setVariables(args, buildProperties(args));
						
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
			System.err.println(e);
		}
		
		
		try {
			
			execute();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			Map<String,List<String>> map = new HashMap<>();
			
			while((line = reader.readNext()) != null) {
				
				if(!map.containsKey(line[0])) {
					ArrayList<String> list = new ArrayList<String>();
					list.add(line[3]);
				} else {
					map.get(line[0]).add(line[3]);
				}
				
			}
			int x = 0;
			for(Entry<String, List<String>> entry: map.entrySet()) {
				if(entry.getValue().size() > 1) {
					System.out.println(entry.toString());
					x++;
				}
			}
			System.out.println(x);
			
		}
		
		
	}

}
