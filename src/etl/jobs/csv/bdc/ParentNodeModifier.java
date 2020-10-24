package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import etl.jobs.mappings.Mapping;

/**
 * This class will remove specified studies based on scenarios listed below 
 * 
 *
 */
public class ParentNodeModifier extends BDCJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5083028424280704439L;

	// global removes ( regex matches ); 
	// removes entire layer and moves lower layer up
	private static Set<String> GLOBAL_REMOVES = new HashSet<String>(Arrays.asList("The dataset provides" ,"This subject phenotype data"));

	// scenario one
	// removes all text after regex match
	// preserves all text before the match
	private static Set<String> S1_STUDIES = new HashSet<String>(Arrays.asList("GENOA"));
	private static final String S1_REGEX = "This dataset include.*";
	
	// scenario two
	// removes all text after regex match
	// preserves all text before the match
	private static Set<String> S2_STUDIES = new HashSet<String>(Arrays.asList("ARIC"));
	private static final String S2_REGEX = "\\..*";
	
	// scenario three
	// removes all text after regex match
	// preserves all text before the match
	private static Set<String> S3_STUDIES = new HashSet<String>(Arrays.asList("WHI"));
	private static final String S3_REGEX = "--.*";	
	
	// scenario four
	// when node contains "Data Contains" only preserve Year #
	private static Set<String> S4_STUDIES = new HashSet<String>(Arrays.asList("CHS"));
	private static final String S4_REGEX = "\\(all > 65 years of age\\).*";
	private static final String S4_REPLACE_VALUE = "(all > 65 years of age)";

	
	public static void main(String[] args) {
		
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}

	private static void execute() throws IOException {
		List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		performGlobalFixes(mappings);
		
		if(S1_STUDIES.contains(TRIAL_ID)) {
			performRegexScenario(mappings,S1_REGEX);
		}
		if(S2_STUDIES.contains(TRIAL_ID)) {
			performRegexScenario(mappings,S2_REGEX);
		}
		if(S3_STUDIES.contains(TRIAL_ID)) {
			performRegexScenario(mappings,S3_REGEX);
		}
		if(S4_STUDIES.contains(TRIAL_ID)) {
			performRegexScenario(mappings,S4_REGEX,S4_REPLACE_VALUE);
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			for(Mapping mapping: mappings) {
				// facts should not be associated with the root node
				if(mapping.getRootNode().split(PATH_SEPARATOR.toString()).length > 2)  {
					buffer.write(mapping.toCSV() + '\n');
				}
			}
			
		}
	}

	private static void performRegexScenario(List<Mapping> mappings, String regex) {
		performRegexScenario(mappings,regex,"");
	}

	private static void performRegexScenario(List<Mapping> mappings, String regex, String replaceVal) {
		
		for(Mapping mapping: mappings) {
			
			String[] nodes = mapping.getRootNode().split(PATH_SEPARATOR.toString());
			
			if(nodes.length < 3) continue;
						
			nodes[2] = nodes[2].replaceAll(regex, replaceVal).trim();
			
			List<String> gl = new ArrayList<String>(Arrays.asList(nodes));
			
			if(nodes[2].trim().isEmpty()) gl.remove(1);
			
			mapping.setRootNode(Mapping.buildConceptPath(gl,PATH_SEPARATOR));
		}
	}

	private static void performGlobalFixes(List<Mapping> mappings) {
		
		for(Mapping mapping: mappings) {
			
			String[] nodes = mapping.getRootNode().split(PATH_SEPARATOR.toString());
			
			if(nodes.length < 3) continue;
			
			String parent = nodes[2];
			
			for(String remove: GLOBAL_REMOVES) {
				
				if(parent.toUpperCase().contains(remove.toUpperCase())) {
					
					List<String> gl = new ArrayList<String>(Arrays.asList(nodes));
					
					gl.remove(2);
					
					mapping.setRootNode(Mapping.buildConceptPath(gl,PATH_SEPARATOR));
					 
				}
			}
			
		}
		 
	}

}

