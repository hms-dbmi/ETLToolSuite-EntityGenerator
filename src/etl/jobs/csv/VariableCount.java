package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class VariableCount extends Job {

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
		
		generateVariableCount();
				
	}

	private static void generateVariableCount() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "allConcepts.csv"))){
			
			String line;
			
			Map<String,Set<String>> map = new HashMap<>();
			
			while((line = buffer.readLine()) != null) {
				
				String[] record = line.split(",");
				
				if(map.containsKey(record[1])) {
					if(record[4].isEmpty()) {
						map.get(record[1]).add("continous");
					} else {
						map.get(record[1]).add(record[4]);
					}
				} else {
					if(record[4].isEmpty()) {
						map.put(record[1], new HashSet<String>(Arrays.asList("continous")));
					} else {
						map.put(record[1], new HashSet<String>(Arrays.asList(record[4])));
					}
				}
				
			}
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "ConceptVariableCount.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
				for(Entry<String,Set<String>> entry: map.entrySet()) {
					String linetowrite = entry.getKey() + "," + entry.getValue().size() + '\n';
					writer.write(linetowrite);
				}
			}
		}
		
	}

	
}
