package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import etl.jobs.mappings.Mapping;

public class GenerateConceptMapping extends BDCJob {

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
			e.printStackTrace();
		}	
	}

	private static void execute() throws IOException {
		// read new mapping file
			
		List<Mapping> mappings = Mapping.generateMappingListForHPDS(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);			
			
		List<Mapping> new_mappings = Mapping.generateMappingListForHPDS(DATA_DIR + TRIAL_ID + "_mapping2.csv", MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		Map<String, String> oldConcepts = buildLookup(mappings);	
		
		Map<String, String> newConcepts = buildLookup(new_mappings);
		
		Set<String[]> conceptMappings = buildConceptMappings(oldConcepts,newConcepts); 
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_concept_mapping.csv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String[] line: conceptMappings) {
				writer.write(toCsv(line));
			}
		}
	}

	private static Set<String[]> buildConceptMappings(Map<String, String> oldConcepts,
			Map<String, String> newConcepts) {
		Set<String[]> set = new HashSet<>();
		for(Entry<String,String> oldConcept: oldConcepts.entrySet()) {
			if(newConcepts.containsKey(oldConcept.getKey())) {
				String[] conceptMapping = new String[2];
				conceptMapping[0] = oldConcept.getValue();
				conceptMapping[1] = newConcepts.get(oldConcept.getKey());
				set.add(conceptMapping);
			}
		}
		return set;
	}

	private static Map<String, String> buildLookup(List<Mapping> mappings) {
		
		
		Map<String, String> map = new HashMap<>();
		
		for(Mapping m: mappings) {
			map.put(m.getKey(), m.getRootNode());
		}
		
		return map ;
	
		
	}

}
//try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_mapping2.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
