package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.jobs.Job;

public class DistinctVariables extends Job {

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
		if(Files.isDirectory(Paths.get(DATA_DIR))) {
			
			File[] dataFiles = new File(DATA_DIR).listFiles();
			
			Set<String> headers = new HashSet<String>();
			List<String> allheaders = new ArrayList<String>(); 
			for(File data: dataFiles) {
				if(!data.getName().contains("phs000007")) continue;
				try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + data.getName()),StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
					
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(data.getAbsolutePath()))){
						
						CSVReader reader = new CSVReader(buffer,'\t', 'ç');
						
						String[] line;
												
						int headerLength = 0;
						
						while((line = reader.readNext()) != null) {
							boolean IS_BAD_MAPPING = false;
							//skip empty lines
							if(line[0].isEmpty()) continue; 
							// skip the comments
							if(line[0].charAt(0) == '#') {
								if(line.length < 2) {
									continue;
								} 
								if(!line[1].contains("phv")) continue;
							}
							
							for(String header: line) {
								headers.add(header);
								allheaders.add(header);
							}
							break;
						}
						
					}
					//System.out.println(headers.size());
					//System.out.println(allheaders.size());
				}
			}
			System.out.println(headers.size());
			System.out.println(allheaders.size());

		}
	}
}
