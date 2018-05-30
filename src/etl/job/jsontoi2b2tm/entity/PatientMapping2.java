package etl.job.jsontoi2b2tm.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.csvreader.CsvReader;

public class PatientMapping2 {
	
	private String patientKey;
	
	private String patientColumn;
	
	private String options;
	
	
	public static String OPTIONS_DELIMITER = ";";
	
	public static String OPTIONS_KV_DELIMITER = ":";
	
	
	public String getPatientKey() {
		return patientKey;
	}

	public void setPatientKey(String patientKey) {
		this.patientKey = patientKey;
	}

	public String getPatientColumn() {
		return patientColumn;
	}

	public void setPatientColumn(String patientColumn) {
		this.patientColumn = patientColumn;
	}

	public String getOptions() {
		return options;
	}

	public void setOptions(String options) {
		this.options = options;
	}

	public static String getOPTIONS_DELIMITER() {
		return OPTIONS_DELIMITER;
	}

	public static void setOPTIONS_DELIMITER(String oPTIONS_DELIMITER) {
		OPTIONS_DELIMITER = oPTIONS_DELIMITER;
	}

	public static String getOPTIONS_KV_DELIMITER() {
		return OPTIONS_KV_DELIMITER;
	}

	public static void setOPTIONS_KV_DELIMITER(String oPTIONS_KV_DELIMITER) {
		OPTIONS_KV_DELIMITER = oPTIONS_KV_DELIMITER;
	}

	public Map<String,String> buildOptions(PatientMapping2 mapping){
		
		Map<String,String> map = new HashMap<String, String>();
		
		String options = mapping.getOptions();

		for(String option: options.split(OPTIONS_DELIMITER)){
			
			String[] kv = option.split(OPTIONS_KV_DELIMITER);
			
			if(kv.length > 1){
				
				map.put(kv[0], kv[1]);

			}
			
		};
		
		return map;
	
	}
	
	public Map<String,String> buildOptions(String str){
		
		Map<String, String> map = new HashMap<String, String>();
		
		String[] split = str.split(OPTIONS_DELIMITER);
		
		for(String option: split){

			map.put(option.split(OPTIONS_KV_DELIMITER)[0], option.split(OPTIONS_KV_DELIMITER)[1]);
			
		}
		
		return map;
	}

	public Set<String> buildOptions(String str, String delimiter){
		
		Set<String> set = new HashSet<String>();
		
		for(String option: str.split(delimiter)){

			set.add(option);
			
		};
		
		return set;
	
	}
	
	public Map<String,String> buildOptions(Mapping mapping, String delimiter){
		
		Map<String,String> map = new HashMap<String, String>();
		
		String options = mapping.getOptions();
		
		for(String option: options.split(";")){

			String[] kv = option.split(delimiter);

			if(kv.length > 0){
				
				map.put(kv[0], kv[1]);

			}
			
		};
		
		return map;
	
	}
	
	private Object getMappingValue(ArrayList<String> patKeys, Object record, String currFullKey) {
		
		if(record instanceof Map) {
			Object object = new Object();
			Map<String, Object> rec = (LinkedHashMap<String,Object>) record;
			
			if(rec.keySet().contains(patKeys.get(0))) {
				Object r = rec.get(patKeys.get(0));
				
				if(r instanceof Map) {
					patKeys.remove(0);
					object = getMappingValue(patKeys, r, currFullKey);
				} else {
					object = getMappingValue(patKeys, r, currFullKey);
				}
			} else {
				object = null;
			}
			return object;
			
		} else if(record instanceof List) {
			List recs = (List) record;
			Object object = new Object();

			for(Object r: recs) {
				object = getMappingValue(patKeys, r, currFullKey);
				if(!object.toString().isEmpty()) break;
			}
			return object;

		} else if(record instanceof String) {
			Object object = new Object();

			object = record.toString();
			
			return object;

		} else if(record == null) {
			Object object = new Object();

			return null;

		} else {
			return null;
		}
	}
	
	public static List<PatientMapping2> generateMappingList(String filePath, char delimiter) throws IOException{
		List<PatientMapping2> mapping = new ArrayList<PatientMapping2>();
				
		CsvReader reader = new CsvReader(filePath);
		
		if(delimiter == ' '){
			
			delimiter = ',';
			
		}
		
		reader.setDelimiter(delimiter);
		
		// skip header
		reader.readHeaders();
		
		while(reader.readRecord()){
			// Check if delimiter exists if so set default.
			if(reader.getValues().length == PatientMapping2.class.getDeclaredFields().length - 2){
				
				PatientMapping2 m = new PatientMapping2();
				
				m.setPatientKey(reader.get(0));				
				m.setPatientColumn(reader.get(1));
				m.setOptions(reader.get(2));
				
				mapping.add(m);
				
			}
		
		}		
		
		return mapping;
		
	}

	@Override
	public String toString() {
		return "PatientMapping2 [patientKey=" + patientKey + ", patientColumn=" + patientColumn + ", options=" + options
				+ "]";
	}
	
}
