package etl.etlinputs.managedinputs.bdc;

import java.util.ArrayList;
import java.util.List;

import etl.etlinputs.managedinputs.GenomicManagedInput;

public class BDCGenomicManagedInput extends GenomicManagedInput {
	
	private String studyIdentifier = "";
	
	private String studyConsent = "";
	
	private String nhlbiAccount = "";
	
	private String sampleMultiLocation = "";

	private String subjectMultiLocation = "";
	
	private String additionalInformation = "";
			
	public BDCGenomicManagedInput(String[] inputCsv) {

		super(inputCsv);
		this.studyAbvName = inputCsv[0];
		this.studyIdentifier = inputCsv[1];
		this.studyConsent = inputCsv[2];
		this.isStudyAnnotated = inputCsv[3];
		this.isStudyProcessed = inputCsv[4];
		this.isStudyIngested = inputCsv[5];
		this.nhlbiAccount = inputCsv[6];
		this.sampleMultiLocation = inputCsv[7];
		this.subjectMultiLocation = inputCsv[8];
		this.additionalInformation = inputCsv[9];
	}
	
	public static List<GenomicManagedInput> buildAll(List<String[]> managedInputs){
		return buildAll(managedInputs,true);
	}
	
	public static List<GenomicManagedInput> buildAll(List<String[]> managedInputs, Boolean skipHeader) {
		List<GenomicManagedInput> inputs = new ArrayList<>();
		for(String[] input: managedInputs) {
			if(input[0].equalsIgnoreCase("Study Abbreviated Name")) continue;
			if(input.length > 3 && input[3].equalsIgnoreCase("no")) continue;
			if (input.length > 4 && input[4].equalsIgnoreCase("yes")) continue;
			inputs.add(new BDCGenomicManagedInput(input));
		}
		return inputs;
	}

	public String getStudyIdentifier() {
		return this.studyIdentifier;
	}

	public void setStudyIdentifier(String studyIdentifier) {
		this.studyIdentifier = studyIdentifier;
	}

	public String getStudyConsent() {
		return this.studyConsent;
	}

	public void setStudyConsent(String studyConsent) {
		this.studyConsent = studyConsent;
	}

	public String getNhlbiAccount() {
		return this.nhlbiAccount;
	}

	public void setNhlbiAccount(String nhlbiAccount) {
		this.nhlbiAccount = nhlbiAccount;
	}

	public String getSampleMultiLocation() {
		return this.sampleMultiLocation;
	}

	public void setSampleMultiLocation(String sampleMultiLocation) {
		this.sampleMultiLocation = sampleMultiLocation;
	}

	public String getSubjectMultiLocation() {
		return this.subjectMultiLocation;
	}

	public void setSubjectMultiLocation(String subjectMultiLocation) {
		this.subjectMultiLocation = subjectMultiLocation;
	}

	public String getAdditionalInformation() {
		return this.additionalInformation;
	}

	public void setAdditionalInformation(String additionalInformation) {
		this.additionalInformation = additionalInformation;
	}

	public String getStudyIdAndConsent() {
		if(this.getStudyConsent().isEmpty()){
			return this.studyIdentifier;
		}
		return this.studyIdentifier + "." + this.studyConsent;
	}

	public String getGlobalConsentCode() {
		if (this.getStudyConsent().isEmpty()) {
			return this.studyIdentifier + ".c1";
		}
		//Special case for if a study is not compliant i.e. CAMP or 1000genomes. Will use value in consent column as consent
		else if (!this.getStudyIdentifier().startsWith("phs")) {
			return this.studyConsent;
		}
		return this.studyIdentifier + "." + this.studyConsent;
	}

	@Override
	public String toString() {
		return "{" +
				" studyIdentifier='" + getStudyIdentifier() + "'" +
				", studyConsent='" + getStudyConsent() + "'" +
				", nhlbiAccount='" + getNhlbiAccount() + "'" +
				", sampleMultiLocation='" + getSampleMultiLocation() + "'" +
				", subjectMultiLocation='" + getSubjectMultiLocation() + "'" +
				", additionalInformation='" + getAdditionalInformation() + "'" +
				"}";
	}

	public static List<String> getPhsAccessions(String studyAbvName, List<BDCGenomicManagedInput> managedInputs) {
		
		List<String> list = new ArrayList<>();
		
		for(BDCGenomicManagedInput input: managedInputs) {
			if(input.getStudyAbvName().equalsIgnoreCase(studyAbvName)) {
				if(input.getStudyConsent().isEmpty()){
					list.add(input.getStudyIdentifier());
				}
				else {
					list.add(input.getStudyIdentifier()+"."+input.getStudyConsent());
				}

			}
		}
		
		return list;
		
	}

	
}
