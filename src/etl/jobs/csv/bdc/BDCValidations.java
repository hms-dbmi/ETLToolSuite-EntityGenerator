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

public class BDCValidations extends BDCJob {

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
		
		Map<String,Set<String>> patientsPerConsentGroup = new HashMap<>();
		
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))){
			
			try (CSVReader reader = new CSVReader(buffer, ',', '\"', 'µ')) {
				String[] line;
				
				String currentNode = "";
				
				while((line = reader.readNext())!=null) {
					// validate distinct patient counts in each study
					if(line[0].equalsIgnoreCase("PATIENT_NUM")) continue;  //skip header
					if(line[1].split("\\\\").length < 2) {
						System.out.println(line[1]);
						continue;
					}
					
					String rootConcept = line[1].split("\\\\")[1];

					if(!rootConcept.equals(currentNode)) {
						System.out.println("Working on " + rootConcept);
						currentNode = rootConcept;
					}
/*				
					if(patientsPerStudy.containsKey(rootConcept)) {
						
						patientsPerStudy.get(rootConcept).add(line[0]);
						
					} else {
						
						patientsPerStudy.put(rootConcept, new HashSet<String>(Arrays.asList(line[0])));
						
					}
*/				
					// validate patients counts per consent group
					if(rootConcept.equalsIgnoreCase("_consents")) {
						
						if(patientsPerConsentGroup.containsKey(line[3])) {
							patientsPerConsentGroup.get(line[3]).add(line[0]);
						} else {
							patientsPerConsentGroup.put(line[3], new HashSet<String>(Arrays.asList(line[0])));
						}
						
					}
/*
					if(rootConcept.equalsIgnoreCase("_studies")) {
						
						_studies.add(line[1].split("\\\\")[2]);
						
					}*/

				}
			}
							
		}/*
		// validate studies from Managed Inputs 
		System.out.println("Validating Managed Inputs.");
		List<BDCManagedInput> managedInputs = ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);
		
		int topmedCount = 0;
		
		int parentCount = 0;
/*		
		for(ManagedInput managedInput : managedInputs) {
			if(managedInput instanceof BDCManagedInput) {
				BDCManagedInput mi = (BDCManagedInput) managedInput;
				if(mi.getReadyToProcess().equalsIgnoreCase("Yes")) {
					//String fullName = mi.getStudyFullName() + " ( " + mi.getStudyIdentifier() + " )";
					String fullName = mi.getStudyIdentifier();
					if(!patientsPerStudy.containsKey(fullName)) {
						
						System.err.println(fullName + " does not have any patients.");
						
					} else {
						if(!_studies.contains(fullName)) {
							System.err.println(fullName + " missing from _studies.");

						}
					}
				}
			}
			
		}		
				
		System.out.println("Studies contains " + _studies.size() + " values.");
		Set<String> patientNums = new HashSet<>();
		for(Entry<String, Set<String>> entry:patientsPerStudy.entrySet()) {
			if(entry.getKey().startsWith("_")) continue;
			for(String patNum: entry.getValue()) {
				if(patientNums.contains(patNum)) {
					System.err.println("Duplicate found in " + entry.getKey() + " - " + patNum);
				}
			}
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Patient_Count_Per_Study.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Entry<String,Set<String>> entry: patientsPerStudy.entrySet()) {
				writer.write(toCsv(new String[] { entry.getKey(), new Integer(entry.getValue().size()).toString()}));
			}
			
		}*/
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Patient_Count_Per_Consents.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Entry<String,Set<String>> entry: patientsPerConsentGroup.entrySet()) {
				writer.write(toCsv(new String[] { entry.getKey(), new Integer(entry.getValue().size()).toString()}));
			}
		}
	}

}
