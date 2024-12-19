package etl.jobs.csv.bdc;

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

import com.opencsv.CSVReader;

public class GenerateConsentMapping extends BDCJob {

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
		
		Map<String,String> patientsPerConsentGroup = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts_merged.csv"))){
			
			CSVReader reader = new CSVReader(buffer, ',', '\"', 'µ');
			String[] line;
			
			String currentNode = "";
			
			while((line = reader.readNext())!=null) {
				// validate distinct patient counts in each study
				if(line[0].equalsIgnoreCase("PATIENT_NUM")) continue;  //skip header
				
				String rootConcept = line[1].split("\",")[0];

				if(!rootConcept.equals(currentNode)) {
					System.out.println("Working on " + rootConcept);
					currentNode = rootConcept;
				}
				if(rootConcept.equalsIgnoreCase("_consents")) {
					String consentGroup = line[1].split("\",")[2].split(",")[0];
					System.out.println("Consent group is " + consentGroup);
					patientsPerConsentGroup.put(line[0], consentGroup);
					
				}

			}
							
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + "Patient_Consent_Map.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Entry<String,String> entry: patientsPerConsentGroup.entrySet()) {
					writer.write(toCsv(new String[] { entry.getKey(), entry.getValue()}));
			}
		}
	}

}
