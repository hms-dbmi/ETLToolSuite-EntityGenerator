package etl.jobs.mappings;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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

public class Mapping implements Cloneable, Comparable<Mapping>{
	
	public static char QUOTED_STRING = '"';

	public static String OPTIONS_DELIMITER = ";";
	
	public static String OPTIONS_KV_DELIMITER = ":";
	
	public static CharSequence PATH_SEPARATOR = "µ";
	
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
	public Mapping(String key, String rootNode, String supPath, String dataType, String options) {
		super();
		this.key = key;
		this.rootNode = rootNode;
		this.supPath = supPath;
		this.dataType = dataType;
		this.options = options;
	}
	
	public Object clone() throws CloneNotSupportedException 
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
		if(new File(filePath).length() == 0) {
			System.err.println(filePath + " is empty returning empty list.");
			return new ArrayList<Mapping>();
		}
		List<Mapping> list = new ArrayList<Mapping>();
		
		
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(filePath))){

			CsvToBean<Mapping> beans = new CsvToBeanBuilder<Mapping>(buffer)
					.withSkipLines(skipheader ? 1 : 0)
					.withQuoteChar(quoteChar)
					.withSeparator(separator)
					.withEscapeChar('¶')
					.withType(Mapping.class)
					.build();			
			
			
			list = beans.parse();

		}
		
		return list;
		
	}
	public static List<Mapping> generateMappingListForHPDS(String filePath, boolean skipheader, char separator,char quoteChar) throws IOException{
		
		if(!Files.exists(Paths.get(filePath))) {
			throw new IOException(filePath + " does not exist.");
		}
		if(new File(filePath).length() == 0) {
			System.err.println(filePath + " is empty returning empty list.");
			return new ArrayList<Mapping>();
		}
		List<Mapping> list = new ArrayList<>();
		/*Set<Mapping> list = new TreeSet<>( new Comparator<Mapping>() {
			
			@Override
			public int compare(Mapping o1, Mapping o2) {
				
				if(o1 == null || o2 == null) return -1;
				
				int conceptpath = o1.getRootNode().compareTo(o2.getRootNode());
				
				return conceptpath;
										
			}
			
		} );*/ 
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(filePath))){

			CsvToBean<Mapping> beans = new CsvToBeanBuilder<Mapping>(buffer)
					.withSkipLines(skipheader ? 1 : 0)
					.withQuoteChar(quoteChar)
					.withSeparator(separator)
					.withType(Mapping.class)
					.build();	
			
			Iterator<Mapping> iter = beans.iterator();
			
			while(iter.hasNext()) {
				
				Mapping mapping = iter.next();
				
				mapping.setRootNode(mapping.getRootNode().replaceAll("\"", ""));
				
				/*
				
				if(rootNode.length() <= 1) continue;
				
				if(!rootNode.substring(0, 1).equals(PATH_SEPARATOR.toString())) throw new IOException("Bad Path separtor in config.  Ensure that the root node pathseparator is correct in the job configuration.");
 				
				String[] nodes = mapping.getRootNode().split(PATH_SEPARATOR.toString());
				/*
				StringBuilder sb = new StringBuilder();
				
				if(nodes.length > 1) {
					
					sb.append(PATH_SEPARATOR);
					
					for(String node: nodes) {
						
						if(node.length() > 250) {
							
							node = node.substring(0, 250) + "...";
							
							sb.append(node);
							
							sb.append(PATH_SEPARATOR);
							
						} else if(!node.isEmpty()){

							sb.append(node);
							
							sb.append(PATH_SEPARATOR);
							
						}
					}
					mapping.setRootNode(sb.toString());
				} else {
					
					continue;
					
				}
				*/
				list.add(mapping);
			}
		}
		Collections.sort(list, new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				return o1.getRootNode().compareTo(o2.getRootNode());
			}
		});
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
	
	public char toCSV(char mAPPING_DELIMITER, char mAPPING_QUOTED_STRING) {
		// TODO Auto-generated method stub
		return 0;
	}
	public String toCSV() {
		return makeStringSafe(key) + ',' + makeStringSafe(rootNode) + ','
				+ makeStringSafe(supPath) + ',' + makeStringSafe(dataType) + ',' + makeStringSafe(options);
	}
	
	public static String makeStringSafe(String string){
		
		if(string != null && !string.isEmpty()){
			if(string != null && !string.isEmpty() && string.substring(0, 1).equals(QUOTED_STRING)){
				return string.replaceAll("\\s{2,}", " ");
			} else {
				return QUOTED_STRING + string.replaceAll("\\s{2,}", " ") + QUOTED_STRING;
			}
		}
		// return empty string not null
		return ""; 
	}

	@Override
	public int compareTo(Mapping o) {
		
		return new Integer(this.getKey().split(":")[1]).compareTo(new Integer(o.getKey().split(":")[1]));
	}
	public int compare(Mapping a, Mapping b) 
    { 
        return a.rootNode.compareTo( b.rootNode ); 
    }
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((options == null) ? 0 : options.hashCode());
		result = prime * result + ((rootNode == null) ? 0 : rootNode.hashCode());
		result = prime * result + ((supPath == null) ? 0 : supPath.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Mapping other = (Mapping) obj;
		if (dataType == null) {
			if (other.dataType != null)
				return false;
		} else if (!dataType.equals(other.dataType))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (options == null) {
			if (other.options != null)
				return false;
		} else if (!options.equals(other.options))
			return false;
		if (rootNode == null) {
			if (other.rootNode != null)
				return false;
		} else if (!rootNode.equals(other.rootNode))
			return false;
		if (supPath == null) {
			if (other.supPath != null)
				return false;
		} else if (!supPath.equals(other.supPath))
			return false;
		return true;
	}
	public static String printHeader() {
		return makeStringSafe("Filename and Variable Position") + ',' + makeStringSafe("Root Node") + ','
				+ makeStringSafe("Supplemental Path") + ',' + makeStringSafe("DataType") + ',' + makeStringSafe("Options");
	}
	
	public static String buildConceptPath(List<String> pathNodes,CharSequence pathSeperator) {
		if(pathNodes.isEmpty()) return "";
		StringBuilder sb = new StringBuilder(); 
		sb.append(pathSeperator);
		for(String node: pathNodes) {
			if(node.isEmpty()) continue;
			
			sb.append(node);
			
			sb.append(pathSeperator);
		}
		
		return sb.toString();
	}
	
}
