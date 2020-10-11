package src.deprecated;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

public class MergeMappings extends Job {

	private static final String OLD_MAPPING_FILE = "mappings/mapping2.csv";

	public static void main(String[] args) {
		
		try {
			setVariables(args, buildProperties(args));
			//setLocalVariables(args);
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

		List<Mapping> oldmappings = Mapping.generateMappingListForHPDS(OLD_MAPPING_FILE, false, ',', '\"');
		List<Mapping> newmappings = Mapping.generateMappingListForHPDS(MAPPING_FILE, false, ',', '\"');
		List<Mapping> mergemapping = new ArrayList<>();
		for(Mapping mapping: newmappings) {
						
			Mapping oldmapping = findMapping(oldmappings,mapping);
			if(oldmapping != null) mergemapping.add(mapping);
			else mergemapping.add(mapping);
		}
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get("mappings/mergedmapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			for(Mapping m: mergemapping) {
				buffer.write(m.toCSV() + '\n');
			}
		}
	}

	private static Mapping findMapping(List<Mapping> oldmappings, Mapping mapping) {
		
		for(Mapping m: oldmappings) {
			if(m.getKey().equals(mapping.getKey())) return m;
		}
		
		return null;
	}

}
