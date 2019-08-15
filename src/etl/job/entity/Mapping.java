package etl.job.entity;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import etl.utils.Utils;

public class Mapping implements Cloneable, Comparable<Mapping>{
	
	public static String OPTIONS_DELIMITER = ";";
	
	public static String OPTIONS_KV_DELIMITER = ":";
	
	@CsvBindByPosition(position = 0, required=false)
	private String key = "";
	@CsvBindByPosition(position = 1, required=false)
	private String rootNode = "";
	@CsvBindByPosition(position = 2, required=false)
	private String supPath = "";
	@CsvBindByPosition(position = 3, required=false)
	private String dataType = "";
	@CsvBindByPosition(position = 4, required=false)
	private String options = "";
	
	public Mapping(){
		
	}
	public Object clone() throws
    CloneNotSupportedException 
	{ 
	return super.clone(); 
	} 
	public Map<String, Mapping> generateMappingHash(String filePath, char delimiter) throws IOException{
		
		Map<String, Mapping> mapping = new HashMap<String, Mapping>();
				
		if(delimiter == ' '){
			
			delimiter = ',';
			
		}
		
		CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(filePath)), delimiter);

		// skip header
		//reader.readHeaders();
		Iterator<String[]> iter = reader.iterator();
		while(iter.hasNext()){
			String[] record = iter.next();
			if(record.length < 5) continue;
			// Check if delimiter exists if so set default.
			if(record.length == Mapping.class.getDeclaredFields().length ){
				
				Mapping m = new Mapping();
				
				m.setKey(record[0]);
				
				m.setRootNode(record[1]);
				
				m.setSupPath(record[2]);
				
				m.setDataType(record[3]);
				
				m.setOptions(record[4]);
								
				mapping.put(m.getKey() + ":" + m.getDataType(), m);
				
			}
		
		}		
		
		return mapping;
		
	}

	
	public static List<Mapping> generateMappingList(String filePath, boolean skipheader, char separator,char quoteChar) throws IOException{
		
		if(!Files.exists(Paths.get(filePath))) {
			throw new IOException(filePath + " does not exist.");
		}
		
		List<Mapping> list = new ArrayList<Mapping>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(filePath))){

			CsvToBean<Mapping> beans = Utils.readCsvToBean(Mapping.class, buffer, quoteChar, separator, skipheader);

			list = beans.parse();
			
		}
		
		return list;
		
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

	@Override
	public int compareTo(Mapping o) {
		
		return this.getKey().compareTo(o.getKey());
	}
	
	
	
}
