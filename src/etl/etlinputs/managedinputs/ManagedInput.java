package etl.etlinputs.managedinputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

import etl.etlinputs.EtlInput;

public abstract class ManagedInput implements EtlInput {

	protected String studyAbvName = "";
	
	protected String readyToProcess = "";
	
	protected String dataProcessed = "";
	
	public ManagedInput(String[] inputCsv) {
	};
	
	public String getStudyAbvName() {
		return studyAbvName;
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

	public void setStudyAbvName(String studyAbvName) {
		this.studyAbvName = studyAbvName;
	}
	
}
