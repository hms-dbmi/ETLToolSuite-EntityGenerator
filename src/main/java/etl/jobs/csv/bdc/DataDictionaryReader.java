package etl.jobs.csv.bdc;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Document;


public class DataDictionaryReader extends BDCJob {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1084095216652318245L;
	/*
	public Map<String, Map<String, String>> buildValueLookup(String[] managedInput) throws IOException {
		
		String subjectMultiFile = BDCJob.getStudySubjectMultiFile(managedInput);
		
		String phtValue = BDCJob.getPht(subjectMultiFile.split("\\."));
		
		String subjectDictionaryFile = BDCJob.FindDictionaryFile(subjectMultiFile);
				
		Document dataDic = BDCJob.buildDictionary(new File(subjectDictionaryFile));

		Map<String, Map<String, String>> valueLookup = (dataDic == null) ? null: buildValueLookup(dataDic);
		
		return valueLookup;

	}*/
	public static Map<String, Map<String, String>> buildValueLookup(File dictFile) throws IOException {
						
		Document dataDic = BDCJob.buildDictionary(dictFile);

		Map<String, Map<String, String>> valueLookup = (dataDic == null) ? new HashMap<>(): buildValueLookup(dataDic);
		
		return valueLookup;

	}
	
	public Map<String, Map<String, String>> buildValueLookup(String phtValue) throws IOException {
		
		File dictFile = BDCJob.FindDictionaryFile(phtValue);
				
		Document dataDic = BDCJob.buildDictionary(dictFile);

		Map<String, Map<String, String>> valueLookup = (dataDic == null) ? null: buildValueLookup(dataDic);
		
		return valueLookup;

	}
}
