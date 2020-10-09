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
	
	public ManagedInput(String[] inputCsv) {
	};
	
	public String getStudyAbvName() {
		return studyAbvName;
	}
}
