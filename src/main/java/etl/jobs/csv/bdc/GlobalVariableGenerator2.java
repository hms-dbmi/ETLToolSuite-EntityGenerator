package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.Job;

public class GlobalVariableGenerator2 extends BDCJob {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7929591347975728181L;

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}	

		try {
			execute();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		List<String[]> patientMappings = Job.getPatientMappings(TRIAL_ID);
		
		BDCManagedInput managedInput = getManagedInput();
		
		Set<AllConcepts> globalConcepts = new HashSet<>();
		
		globalConcepts.addAll(buildStudies(managedInput, patientMappings));

		globalConcepts.addAll(buildStudiesConsents(managedInput, patientMappings));
		
		globalConcepts.addAll(buildConsents(managedInput, patientMappings));
		globalConcepts.addAll(buildSubjectAccessionIds(managedInput, patientMappings));
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_GLOBALVars.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(AllConcepts ac: globalConcepts) {
				ac.setNvalNum("");
				writer.write(ac.toCSV());
			}
		}

	}
	
	
	private static Collection<? extends AllConcepts> buildSubjectAccessionIds(BDCManagedInput managedInput,
			List<String[]> patientMappings) {
		String root = "µ_Topmed Study Accession with Subject IDµ";
				
		Set<AllConcepts> acs = new HashSet<>();
		
		for(String[] pm: patientMappings) {
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath(root);
				ac.setTvalChar(managedInput.getStudyIdentifier() + ".v1_" + pm[0]);
				ac.setStartDate("0");
				acs.add(ac);
			}
			
		}		
		return acs;
	}
	
	private static Collection<? extends AllConcepts> buildConsents(BDCManagedInput managedInput,
			List<String[]> patientMappings) {
		String studyConsent = managedInput.getStudyIdentifier() + ".c1";
				
		Set<AllConcepts> acs = new HashSet<>();
		
		for(String[] pm: patientMappings) {
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath("µ_consentsµ");
				ac.setTvalChar(studyConsent);
				ac.setStartDate("0");
				acs.add(ac);
			}
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath("µ_topmed_consentsµ");
				ac.setTvalChar(studyConsent);
				ac.setStartDate("0");
				acs.add(ac);
			}
		}		
		return acs;
	}

	private static Collection<? extends AllConcepts> buildStudiesConsents(BDCManagedInput managedInput,
			List<String[]> patientMappings) {
		
		String studyConsent = managedInput.getStudyAbvName() + " (" + managedInput.getStudyIdentifier() + ")";
		
		String consentShortName = "GRU";
		
		Set<AllConcepts> acs = new HashSet<>();
		
		for(String[] pm: patientMappings) {
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath("µ_studies_consentsµ");
				
				ac.setTvalChar("TRUE");
				ac.setStartDate("0");
				acs.add(ac);
			}
		}
		
		for(String[] pm: patientMappings) {
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath("µ_studies_consentsµ" + studyConsent + "µ");
				ac.setTvalChar("TRUE");
				ac.setStartDate("0");
				acs.add(ac);
			}
		}
		for(String[] pm: patientMappings) {
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath("µ_studies_consentsµ" + studyConsent + "µ" + consentShortName + "µ");
				ac.setTvalChar("TRUE");
				ac.setStartDate("0");
				acs.add(ac);
			}
		}
		return acs;
		
	}

	private static Collection<? extends AllConcepts> buildStudies(BDCManagedInput managedInput, List<String[]> patientMappings) {
		String fullname = managedInput.getStudyIdentifier() + " ( " + managedInput.getStudyIdentifier() + " )";
		Set<AllConcepts> acs = new HashSet<>();
		for(String[] pm: patientMappings) {
			if(NumberUtils.isCreatable(pm[2])) {
				AllConcepts ac = new AllConcepts();
				ac.setPatientNum(new Integer(pm[2]));
				ac.setConceptPath("µ_studiesµ" + fullname + "µ");
				ac.setTvalChar("TRUE");
				ac.setStartDate("0");
				acs.add(ac);
			}
		}
		return acs;
	}

	private static BDCManagedInput getManagedInput() throws IOException {
		List<BDCManagedInput> managedInputs = getManagedInputs();
		
		for(BDCManagedInput managedInput: managedInputs) {
			if(managedInput.getStudyAbvName().toUpperCase().equals(TRIAL_ID.toUpperCase())) return managedInput;
		}
		
		return null;
	}

}
