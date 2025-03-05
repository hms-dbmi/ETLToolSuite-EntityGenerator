package etl.etlinputs.managedinputs.bdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.ManagedInputFactory;

public class BDCManagedInput extends ManagedInput {
	
	private String studyIdentifier = "";
	
	private String studyType = "";
	
	private String studyFullName = "";
	
	private String dataType = "";
	
	private String isHarmonized = "";

	private String authZ = "";

	private String version = "";

	private String phase = "";

	private String hasMulti = "";

	private String subjectType = "";

	private String bdcPrograms = "";

	private String additionalInfoLink = "";

	private String additionalInfo = "";

	private String studyLink = "";

	private String requestAccessText = "";

			
	public BDCManagedInput(Map<String,Integer> headersMap, String[] inputCsv) {
		super(inputCsv);
		this.studyAbvName = inputCsv[headersMap.get("Study Abbreviated Name")];
		this.studyIdentifier = inputCsv[headersMap.get("Study Identifier")];
		this.studyType = inputCsv[headersMap.get("Study Type")];
		this.studyFullName = inputCsv[headersMap.get("Study Full Name")];
		this.dataType = inputCsv[headersMap.get("Data Type")];
		this.isHarmonized = inputCsv[headersMap.get("DCC Harmonized")];
		this.readyToProcess = inputCsv[headersMap.get("Data is ready to process")];
		this.dataProcessed = inputCsv[headersMap.get("Data Processed")];
		this.authZ = "/programs/"+inputCsv[headersMap.get("Gen3 Authz Program Name")]+"/projects/"+inputCsv[headersMap.get("Gen3 Authz Project Name")];
		if(!this.studyType.equals("PUBLIC")){
			this.authZ = this.authZ + "_";
		}
		this.version = inputCsv[headersMap.get("Version")];	
		this.phase = inputCsv[headersMap.get("Phase")];
		this.hasMulti = inputCsv[headersMap.get("Has Multi")];
		this.subjectType = inputCsv[headersMap.get("Subject Type(s)")];
		this.bdcPrograms = inputCsv[headersMap.get("BDC Program(s)")];
		this.additionalInfoLink = inputCsv[headersMap.get("Additional Information Link (URL)")];
		this.additionalInfo = inputCsv[headersMap.get("Additional Information Link (Label)")];
		this.requestAccessText = inputCsv[headersMap.get("Request Access Text")];
	}
	
	public static List<BDCManagedInput> buildAll(List<String[]> managedInputs){
		List<BDCManagedInput> inputs = new ArrayList<>();
		String[] headers = {};
		Map<String,Integer> headersMap = null;
		
		if(managedInputs.get(0)[0].equalsIgnoreCase("Study Abbreviated Name")) 
		{
			headers = managedInputs.get(0);
			headersMap = buildInputsHeaderMap(headers);
		}
		else {
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./data/Managed_Inputs_Headers.csv"))) {
			
			@SuppressWarnings("resource")
			List<String[]> records = new CSVReader(buffer).readAll();
			if (records.get(0)[0].equalsIgnoreCase("Study Abbreviated Name")){
				headers = records.get(0);
				headersMap = buildInputsHeaderMap(headers);
				}
			}
			catch(IOException e) {
				System.out.println("NO MANAGED INPUT HEADER OR HEADER FILE FOUND");
				e.printStackTrace();
			}	
		}
		try {
			for(String[] input: managedInputs) {
				if(input[headersMap.get("Data is ready to process")].equalsIgnoreCase("yes")) {
					inputs.add(new BDCManagedInput(headersMap,input));
				}
			}
			return inputs;
		} catch (NullPointerException e) {
			System.out.println("MANAGED INPUTS NOT PARSED CORRECTLY");
			e.printStackTrace();
		}
		return null;
	}

	public static Map<String,Integer> buildInputsHeaderMap(String[] headers){
		Map<String,Integer> inputsHeaders = new HashMap<String,Integer>();
		for (int i = 0; i<headers.length; i++){
			inputsHeaders.put(headers[i],i);
		}
		return inputsHeaders;
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
	
	public String getAuthZ(){
		return authZ;
	}


	public void setStudyIdentifier(String studyIdentifier) {
		this.studyIdentifier = studyIdentifier;
	}

	public void setStudyType(String studyType) {
		this.studyType = studyType;
	}

	public void setStudyFullName(String studyFullName) {
		this.studyFullName = studyFullName;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public void setIsHarmonized(String isHarmonized) {
		this.isHarmonized = isHarmonized;
	}

	public void setAuthZ(String authZ) {
		this.authZ = authZ;
	}
	
	public String getVersion() {
		return this.version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getPhase() {
		return this.phase;
	}

	public void setPhase(String phase) {
		this.phase = phase;
	}
	public String getHasMulti() {
		return this.hasMulti;
	}

	public void setHasMulti(String hasMulti) {
		this.hasMulti = hasMulti;
	}
	public String getSubjectType() {
		return this.subjectType;
	}

	public void setSubjectType(String subjectType) {
		this.subjectType = subjectType;
	}
	public String getAdditionalInfoLink() {
		return this.additionalInfoLink;
	}

	public void setAdditionalInfoLink(String additionalInfoLink) {
		this.additionalInfoLink = additionalInfoLink;
	}

	public String getAdditionalInfo() {
		return this.additionalInfo;
	}

	public void setAdditionalInfo(String additionalInfo) {
		this.additionalInfo = additionalInfo;
	}

	public String getStudyLink() {
		return this.studyLink;
	}

	public void setStudyLink(String studyLink) {
		this.studyLink = studyLink;
	}

	public String getRequestAccessText() {
		return this.requestAccessText;
	}

	public void setRequestAccessText(String requestAccessText) {
		this.requestAccessText = requestAccessText;
	}

	public String getBdcPrograms() {
		return this.bdcPrograms;
	}

	public void setBdcPrograms(String bdcPrograms) {
		this.bdcPrograms = bdcPrograms;
	}
	@Override
	public String toString() {
		return "BDCManagedInput [studyIdentifier=" + studyIdentifier + ", studyType=" + studyType + ", studyFullName="
				+ studyFullName + ", dataType=" + dataType + ", isHarmonized=" + isHarmonized + ", studyAbvName=" + studyAbvName + "]";
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
//use for classes/methods which depend on subject multi. Mostly used for consent generations
	public boolean hasSubjectMultiFile() {
		if(this.hasMulti.equalsIgnoreCase("Yes")) return true;
		return false;

	}
//use for classes/methods which require compliant format for all data
	public boolean isDBGapCompliant() {
		if(this.studyType.equalsIgnoreCase("topmed")) return true;
		if(this.studyType.equalsIgnoreCase("parent")) return true;
		if(this.studyType.equalsIgnoreCase("substudy")) return true;
		return false;
	}
	
}
