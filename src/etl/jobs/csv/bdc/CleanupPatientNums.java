package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.PatientMapping;

public class CleanupPatientNums extends BDCJob {

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
		
		List<BDCManagedInput> managedInputs = getManagedInputs();
		
		Map<String, Map<String, String>> patientMappings = BDCJob.getPatientMappings();
		
		
		Set<PatientMapping> updatedPms = new HashSet<>();
		
		Map<String,Set<PatientMapping>> toWrite = new HashMap<>();
		
		for(BDCManagedInput managedInput: managedInputs) {
			
			if(NON_DBGAP_STUDY.contains(managedInput.getStudyAbvName().toUpperCase())) continue;
			
			Set<PatientMapping> arr = toWrite.containsKey(managedInput.getStudyAbvName()) ? toWrite.get(managedInput.getStudyAbvName()) : new HashSet<>();
			
			List<String[]> multifile = readMultiFile(managedInput);
			
			arr.addAll(cleanPatientMapping(multifile,patientMappings,managedInput));
					
			toWrite.put(managedInput.getStudyAbvName(), arr);
			
		}
		
		for(Entry<String, Set<PatientMapping>> entry: toWrite.entrySet()) {
			writePm(Paths.get(WRITE_DIR + entry.getKey() + "_PatientMapping.v2.csv"),entry.getValue());
		}
	}

	private static void writePm(Path path, Set<PatientMapping> updatedPms) throws IOException {
		try(BufferedWriter buffer = Files.newBufferedWriter(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
			for(PatientMapping pm: updatedPms) {
				buffer.write(pm.toCSV());
			}
		}
		
	}


	private static Set<PatientMapping> cleanPatientMapping(List<String[]> multifile,
			Map<String, Map<String, String>> patientMappings, BDCManagedInput managedInput) {
		
		Set<PatientMapping> newPms = new HashSet<>();
		
		Set<String> multiLookup = new HashSet<>();
		
		for(String[] multi: multifile) {
			multiLookup.add(multi[0]);
		}
		
		Map<String, String> map = patientMappings.get(managedInput.getStudyAbvName());
		
		for(Entry<String,String> entry: map.entrySet()) {
			
			String sourceId = entry.getKey();
			
			boolean patientExists = multiLookup.contains(sourceId);
			
			PatientMapping pm = new PatientMapping(new String[] { sourceId, managedInput.getStudyAbvName(), entry.getValue() });
			
			if(patientExists) newPms.add(pm);
			
		}
		 
		return newPms;
	}

	private static List<String[]> readMultiFile(BDCManagedInput managedInput) throws IOException {
		File file = new File(DATA_DIR + BDCJob.getStudySubjectMultiFile(managedInput));
		
		if(file.isFile()) {
		
			return BDCJob.readRawBDCDataset(file, true).readAll();
			
		} else {
			return null;
		}
		
	}

}
