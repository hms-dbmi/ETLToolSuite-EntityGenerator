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
		this.readyToProcess = inputCsv[7];
		this.dataProcessed = inputCsv[8];
	}
	
	public static List<ManagedInput> buildAll(List<String[]> managedInputs){
		return buildAll(managedInputs,true);
	}
	
	public static List<ManagedInput> buildAll(List<String[]> managedInputs, Boolean skipHeader) {
		List<ManagedInput> inputs = new ArrayList<>();
		for(String[] input: managedInputs) {
			if(input[0].equalsIgnoreCase("Study Abbreviated Name")) continue;
			if(input.length > 7 && input[7].equalsIgnoreCase("no")) continue; 
			//if(input.length > 8 && input[8].equalsIgnoreCase("no")) continue;
			inputs.add(new BDCManagedInput(input));
		}
		return inputs;
	}

	public String getReadyToProcess() {
		return readyToProcess;
	}

	public void setReadyToProcess(String readyToProcess) {
		this.readyToProcess = readyToProcess;
	}

	public String getDataProcessed() {
		return dataProcessed;
	}

	public void setDataProcessed(String dataProcessed) {
		this.dataProcessed = dataProcessed;
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

	@Override
	public String toString() {
		return "BDCManagedInput [studyIdentifier=" + studyIdentifier + ", studyType=" + studyType + ", studyFullName="
				+ studyFullName + ", dataType=" + dataType + ", isHarmonized=" + isHarmonized + ", phsSubjectIdColumn="
				+ phsSubjectIdColumn + ", studyAbvName=" + studyAbvName + "]";
	}

	public static List<String> getPhsAccessions(String studyAbvName, List<BDCManagedInput> managedInputs) {
		
		List<String> list = new ArrayList<>();
		
		for(BDCManagedInput input: managedInputs) {
			if(input.getStudyAbvName().equalsIgnoreCase(studyAbvName)) {
				list.add(input.getStudyIdentifier());
			}
		}
		
		return list;
		
	}


	
}
