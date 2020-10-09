package etl.etlinputs.managedinputs.bdc;

import java.util.ArrayList;
import java.util.List;

import etl.etlinputs.managedinputs.ManagedInput;

public class BDCManagedInput extends ManagedInput {
	
	private String studyIdentifier = "";
	
	private String studyType = "";
	
	private String studyFullName = "";
	
	private String dataType = "";
	
	private String isHarmonized = "";
	
	private String phsSubjectIdColumn = "";

	public BDCManagedInput(String[] inputCsv) {
		super(inputCsv);
		this.studyAbvName = inputCsv[0];
		this.studyIdentifier = inputCsv[1];
		this.studyType = inputCsv[2];
		this.studyFullName = inputCsv[3];
		this.dataType = inputCsv[4];
		this.isHarmonized = inputCsv[5];
		this.phsSubjectIdColumn = inputCsv[6];
	}
	
	public static List<ManagedInput> buildAll(List<String[]> managedInputs) {
		List<ManagedInput> inputs = new ArrayList<>();
		for(String[] input: managedInputs) {
			inputs.add(new BDCManagedInput(input));
		}
		return inputs;
	}

	public String getStudyIdentifier() {
		return studyIdentifier;
	}

	public String getStudyType() {
		return studyType;
	}

	public String getStudyFullName() {
		return studyFullName;
	}

	public String getDataType() {
		return dataType;
	}

	public String getIsHarmonized() {
		return isHarmonized;
	}

	public String getPhsSubjectIdColumn() {
		return phsSubjectIdColumn;
	}	
	
}
