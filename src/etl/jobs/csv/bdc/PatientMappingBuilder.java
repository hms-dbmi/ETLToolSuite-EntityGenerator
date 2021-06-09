package etl.jobs.csv.bdc;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.csv.GenerateAllConcepts;
import etl.jobs.jobproperties.JobProperties;
import etl.jobs.mappings.PatientMapping;

public class PatientMappingBuilder extends BDCJob {

	public static void main(String[] args) {
		
		try {

			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
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
		
		List<PatientMapping> patientMappings = PatientMapping.readPatientMappingFile(PATIENT_MAPPING_FILE);
		
		List<BDCManagedInput> managedInputs = getManagedInputs();
		
		List<String> accessions = BDCManagedInput.getPhsAccessions(TRIAL_ID, managedInputs);
		 
		if(patientMappings.isEmpty()) {
			System.out.println("Patient Mapping does not exist.  Building new Patient Mapping.");
		}
		
		File[] files = new File(DATA_DIR).listFiles(new FilenameFilter() {
			
			//apply a filter
			@Override
			public boolean accept(File dir, String name) {
				boolean result;
				if(name.toUpperCase().contains("SUBJECT.MULTI")){
					result=true;
				}
				else{
					result=false;
				}
				return result;
			}
		
		});
		
		for(File f: files) {
			String phs = BDCJob.getPhs(f.getName());
			if(accessions.contains(phs)) {
				
			}
		}
		
	}

	protected static void setLocalVariables(String[] args, JobProperties properties) throws Exception {

	}
	
}
