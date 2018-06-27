package etl.job.jsontoi2b2tm.entity;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.csvreader.CsvReader;

import etl.data.export.entities.Entity;

public class Mapping {
	
	public static String OPTIONS_DELIMITER = ";";
	
	public static String OPTIONS_KV_DELIMITER = ":";
	
	private String key;
	private String rootNode;
	private String supPath;
	private String dataType;
	private String options;
	
	public Mapping(){
		
	}

	public Map<String, Mapping> generateMappingHash(String filePath, char delimiter) throws IOException{
		
		Map<String, Mapping> mapping = new HashMap<String, Mapping>();
				
		CsvReader reader = new CsvReader(filePath);

		if(delimiter == ' '){
			
			delimiter = ',';
			
		}
		
		reader.setDelimiter(delimiter);
		
		// skip header
		reader.readHeaders();
		
		
		while(reader.readRecord()){
			// Check if delimiter exists if so set default.
			if(reader.getValues().length == Mapping.class.getDeclaredFields().length ){
				
				Mapping m = new Mapping();
				
				m.setKey(reader.get(0));
				
				m.setRootNode(reader.get(1));
				
				m.setSupPath(reader.get(2));
				
				m.setDataType(reader.get(3));
				
				m.setOptions(reader.get(4));
								
				mapping.put(m.getKey() + ":" + m.getDataType(), m);
				
			}
		
		}		
		
		return mapping;
		
	}

	
	public static List<Mapping> generateMappingList(String filePath, boolean skipheader, char delimiter,char quotedString) throws IOException{
		List<Mapping> mapping = new ArrayList<Mapping>();
				
		CsvReader reader = new CsvReader(filePath);
		
		reader.setDelimiter(delimiter);
		reader.setTextQualifier(quotedString);
		// skip header
		if(skipheader) {
			reader.readHeaders();
		}	
		while(reader.readRecord()){
			// Check if delimiter exists if so set default.
			if(reader.getValues().length == Mapping.class.getDeclaredFields().length - 2){
				
				Mapping m = new Mapping();
				
				m.setKey(reader.get(0));
				
				m.setRootNode(reader.get(1));
				
				m.setSupPath(reader.get(2));
				
				m.setDataType(reader.get(3));
				
				m.setOptions(reader.get(4));

				mapping.add(m);
				
			}
		
		}		
		
		return mapping;
		
	}

	public static Set<String> keySet(List<Mapping> mappings){
		Set<String> set = new HashSet<String>();
		
		for(Mapping mapping:mappings) {
			if(set.contains(mapping.getKey())) System.err.println(mapping.getKey());
			set.add(mapping.getKey());
		}
		return set;
	}
	
	public Set<String> keySet(Map<String, Mapping> mapping){
		Set<String> keySet = new HashSet<String>();
		
		mapping.forEach((k,v) -> {
			keySet.add(v.getKey());
		});
		
		return keySet;
	}
	
	public Set<String> keySet(Collection<Mapping> mapping){
		Set<String> keySet = new HashSet<String>();
		
		mapping.forEach(item -> {
			keySet.add(item.getKey());
		});
		
		return keySet;
	}
	
	public Map<String,String> buildOptions(Mapping mapping){
		
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
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getRootNode() {
		return rootNode;
	}

	public void setRootNode(String rootNode) {
		this.rootNode = rootNode;
	}

	public String getSupPath() {
		return supPath;
	}

	public void setSupPath(String supPath) {
		this.supPath = supPath;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getOptions() {
		return options;
	}

	public void setOptions(String options) {
		this.options = options;
	}

	@Override
	public String toString() {
		return "Mapping [key=" + key + ", rootNode=" + rootNode + ", supPath="
				+ supPath + ", dataType=" + dataType + ", options=" + options
				+ "]";
	}
	
	public String toCSV() {
		return makeStringSafe(key) + ',' + makeStringSafe(rootNode) + ','
				+ makeStringSafe(supPath) + ',' + makeStringSafe(dataType) + ',' + makeStringSafe(options);
	}
	
	public String makeStringSafe(String string){
		
		if(string != null && !string.isEmpty()){
			if(string != null && !string.isEmpty() && string.substring(0, 1).equals("`")){
				return string.replaceAll("\\s{2,}", " ");
			} else {
				return '`' + string.replaceAll("\\s{2,}", " ") + '`';
			}
		}
		// return empty string not null
		return ""; 
	}
}
