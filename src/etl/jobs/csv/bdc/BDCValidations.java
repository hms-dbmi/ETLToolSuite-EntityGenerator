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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

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
		Map<String,Set<String>> patientsPerStudy = new HashMap<>();
		
		Map<String,Set<String>> patientsPerConsentGroup = new HashMap<>();
		
		Set<String> _studies = new HashSet<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))){
			
			CSVReader reader = new CSVReader(buffer, ',', '\"', '\\');
			String[] line;
			
			String currentNode = "";
			
			while((line = reader.readNext())!=null) {
				// validate distinct patient counts in each study
				String rootConcept = line[1].split("µ")[1];

				if(!rootConcept.equals(currentNode)) {
					System.out.println("Working on " + rootConcept);
					currentNode = rootConcept;
				}
				
				if(patientsPerStudy.containsKey(rootConcept)) {
					
					patientsPerStudy.get(rootConcept).add(line[0]);
					
				} else {
					
					patientsPerStudy.put(rootConcept, new HashSet<String>(Arrays.asList(line[0])));
					
				}
				
				// validate patients counts per consent group
				if(rootConcept.equalsIgnoreCase("_consents")) {
					
					if(patientsPerConsentGroup.containsKey(line[3])) {
						patientsPerConsentGroup.get(line[3]).add(line[0]);
					} else {
						patientsPerConsentGroup.put(line[3], new HashSet<String>(Arrays.asList(line[0])));
					}
					
				}

				if(rootConcept.equalsIgnoreCase("_studies")) {
					
					_studies.add(line[1].split("µ")[2]);
					
				}

			}
							
		}
		// validate studies from Managed Inputs 
		System.out.println("Validating Managed Inputs.");
		List<ManagedInput> managedInputs = ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);
		
		int topmedCount = 0;
		
		int parentCount = 0;
		
		for(ManagedInput managedInput : managedInputs) {
			if(managedInput instanceof BDCManagedInput) {
				BDCManagedInput mi = (BDCManagedInput) managedInput;
				if(mi.getReadyToProcess().equalsIgnoreCase("Yes")) {
					String fullName = mi.getStudyFullName() + " ( " + mi.getStudyIdentifier() + " )";
					
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
			
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Patient_Count_Per_Consents.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Entry<String,Set<String>> entry: patientsPerConsentGroup.entrySet()) {
				writer.write(toCsv(new String[] { entry.getKey(), new Integer(entry.getValue().size()).toString()}));
			}
		}
	}

}
