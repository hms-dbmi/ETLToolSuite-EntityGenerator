package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.Mapping;

public class RootNodeGenerator extends BDCJob {

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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void execute() throws IOException {
		List<BDCManagedInput> managedInputs = BDCJob.getManagedInputs();
		
		Map<String, Map<String, String>> patientMappings = BDCJob.getPatientMappings();
		
		// Map of rootnode = key, list of patients = value
		Map<String,List<String>> patientSets = getPatientSets(managedInputs,patientMappings);
		
	}

	private static Map<String, List<String>> getPatientSets(List<BDCManagedInput> managedInputs,
			Map<String, Map<String, String>> patientMappings) throws IOException {
		
		Map<String, List<String>> map = new HashMap<>();
		
		for(BDCManagedInput managedInput: managedInputs) {
			if(!managedInput.getReadyToProcess().toLowerCase().startsWith("y")) continue;
			if(BDCJob.NON_DBGAP_STUDY.contains(managedInput.getStudyAbvName())) continue;
			String rootNode = managedInput.getStudyFullName() + " ( " + managedInput.getStudyIdentifier() + " )";
			if(patientMappings.containsKey(managedInput.getStudyAbvName())) {
			
				try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + managedInput.getStudyIdentifier() + "rootnode.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				
					// 

					
					List<String> dbgapSubjIds = BDCJob.getDbgapSubjIdsFromRawData(managedInput);
					
					for(Entry<String,String> entry: patientMappings.get(managedInput.getStudyAbvName()).entrySet()) {
						if(dbgapSubjIds.contains(entry.getKey())) {
							String[] line = new String[] { entry.getValue(), "TRUE" };
							writer.write(toCsv(line));
						}
					}
					
				}
				try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "rootnode_mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND )) {
					Mapping mapping = new Mapping( managedInput.getStudyIdentifier() + "rootnode.csv:1", "µ_studiesµ" + rootNode + "µ", "", "TEXT", "");
					writer.write(mapping.toCSV() + '\n');
					
				}
			}
			
			ArrayList<String> list = new ArrayList<>();
			list.addAll(patientMappings.get(managedInput.getStudyAbvName()).values());
			map.put(managedInput.getStudyFullName() + " ( " + managedInput.getStudyIdentifier() + " )"
					, list);
			
		}
		
		return map;
	}

}
