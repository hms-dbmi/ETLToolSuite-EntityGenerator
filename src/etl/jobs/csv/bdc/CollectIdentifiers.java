package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;

import etl.jobs.Job;
import etl.jobs.jobproperties.JobProperties;

public class CollectIdentifiers extends Job {

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
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		Map<String,Set<String>> fileIdentifiers = getFileIdentifiers(); 
		
		writeIdentifiers(fileIdentifiers);
	}

	private static void writeIdentifiers(Map<String, Set<String>> fileIdentifiers) throws IOException {
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "file_identifiers.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			for(Entry<String, Set<String>> entry: fileIdentifiers.entrySet()) {
				for(String fname: entry.getValue()) {
					StringBuilder sb = new StringBuilder();
					sb.append(fname);
					sb.append(",");
					sb.append(entry.getKey());
					sb.append("\n");
					
					buffer.write(sb.toString());
				}
			}
			
			
		}
		
	}

	private static Map<String, Set<String>> getFileIdentifiers() throws IOException {
		
		Map<String,Set<String>> map = new HashMap<>();
		
		File[] dataDir = new File(DATA_DIR).listFiles();
		
		for(File f: dataDir) {
			
			if(f.getName().endsWith("txt")) {
				
				String[] headers = BDCJob.getDataHeaders(f);
				if(headers == null) continue;

				if(headers.length < 3) continue;
				if(headers[0].isEmpty()) continue;
				
				if(map.containsKey(headers[0])) {
					map.get(headers[0]).add(f.getName()); 
				} else {
					Set<String> set = new HashSet<>();
					set.add(f.getName());
					map.put(headers[0], set);
				}
				
			}
		}
		
		return map;
	}

	private static void setLocalVariables(String[] args, JobProperties buildProperties) {
		// TODO Auto-generated method stub
		
	}

}
