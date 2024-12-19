package etl.etlinputs.managedinputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

import etl.etlinputs.EtlInput;

public abstract class GenomicManagedInput implements EtlInput {

	protected String studyAbvName = "";
	
	protected String isStudyAnnotated = "";

	protected String isStudyProcessed = "";

	protected String isStudyIngested = "";
	
	public GenomicManagedInput(String[] inputCsv) {
	};
	
	public String getStudyAbvName() {
		return studyAbvName;
	}

	public void setStudyAbvName(String studyAbvName) {
		this.studyAbvName = studyAbvName;
	}

	public String getIsStudyAnnotated(){
		return isStudyAnnotated;
	}

	public void setIsStudyAnnotated(String isStudyAnnotated) {
		this.isStudyAnnotated = isStudyAnnotated;
	}

	public String getIsStudyProcessed() {
		return isStudyProcessed;
	}

	public void setIsStudyProcessed(String isStudyProcessed) {
		this.isStudyProcessed = isStudyProcessed;
	}
	
	public String getIsStudyIngested() {
		return isStudyIngested;
	}

	public void setIsStudyIngested(String isStudyIngested) {
		this.isStudyIngested = isStudyIngested;
	}
	
}
