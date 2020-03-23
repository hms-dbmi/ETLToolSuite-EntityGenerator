package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import etl.jobs.jobproperties.JobProperties;

public class ConsentCountValidation extends Job {

	private static final CharSequence CONSENTS_VARIABLE = "Study Accession with Consent Code";
	private static String ALL_CONCEPTS_FILE = "GLOBAL_allConcepts.csv";

	public static void main(String[] args) {
		try {

			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
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
		
		Map<String,Set<String>> consentCounts = buildConsentCounts();
		
		writeConsentValidation(consentCounts);
		
	}

	private static void writeConsentValidation(Map<String, Set<String>> consentCounts) throws IOException {
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "ConsentValidation.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			String header = "\"Accession with Consent Group\"," + "\"Patient Count\"\n";
			
			buffer.write(header);
			
			for(Entry<String,Set<String>> entry: consentCounts.entrySet()) {
			
				String line = entry.getKey() + DATA_SEPARATOR + entry.getValue().size() + '\n';
				
				buffer.write(line);
				
			}
			
		}
		
	}

	private static Map<String, Set<String>> buildConsentCounts() throws IOException {
		// read the global all concepts to collect 
		// these values are merged into allConcepts that will be generated in javabins.
		Map<String,Set<String>> consents = new TreeMap<>(); 
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + ALL_CONCEPTS_FILE))){
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] record = line.split(new Character(DATA_SEPARATOR).toString());
				
				if(!record[1].contains(CONSENTS_VARIABLE)) continue;
				
				if(consents.containsKey(record[3])) {
					
					consents.get(record[3]).add(record[0]);
					
				} else {
					
					consents.put(record[3], new TreeSet<String>( Arrays.asList(record[0]))); //record[0]);
					
				}
				
			}
			
		}
		
		return consents;
		
	}

	protected static void setLocalVariables(String[] args, JobProperties properties) throws Exception {
		
		for(String arg: args) {
			
			if(arg.equalsIgnoreCase("-allconceptsfile")){
				
				ALL_CONCEPTS_FILE = checkPassedArgs(arg, args);			
				
			}
			
		}
		
	}
	
}
