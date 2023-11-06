package etl.jobs.csv.bdc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

public class AppendSampleId extends BDCJob {

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
		
		List<BDCManagedInput> managedInputs = BDCJob.getManagedInputs();
		
		Map<String,File[]> sampleFiles = getSampleFiles(managedInputs);
		
		appendSampleIds(sampleFiles);
	}

	private static void appendSampleIds(Map<String, File[]> sampleFiles) {
		// TODO Auto-generated method stub
		
	}

	private static Map<String, File[]> getSampleFiles(List<BDCManagedInput> managedInputs) throws IOException {
		Map<String, List<File>> map = new HashMap<>();
		
		for(BDCManagedInput managedInput: managedInputs) {
			
			List<File> files = map.containsKey(managedInput.getStudyAbvName()) ? map.get(managedInput.getStudyAbvName()) : new ArrayList<File>();
			
			File f = new File(DATA_DIR);
			
			String sample = BDCJob.getStudySampleMultiFile(managedInput);
			
			if(!sample.isEmpty()) files.add(new File(DATA_DIR + sample));
			
		}
		return null;
	}

}
