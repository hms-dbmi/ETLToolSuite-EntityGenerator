package src.deprecated;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

public class AllConceptPrep extends Job {

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
		
		if(Files.isDirectory(Paths.get(MAPPING_DIR))) {
		
			for(File f : new File(MAPPING_DIR).listFiles()){
				
				if(f.getName().contains("mapping.csv")) {
					
					Mapping mapping = Mapping.generateMappingList(f.getAbsolutePath(),MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING).get(0);
					
					
					
				}
				
			}
			
		}
				
	}

}
