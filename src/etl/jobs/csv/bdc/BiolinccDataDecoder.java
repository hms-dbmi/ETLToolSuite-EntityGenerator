package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;

import etl.jobs.mappings.Mapping;

public class BiolinccDataDecoder extends BDCJob {
    /// < FILE , MISSING HEADER >
	private static Map<String,Set<String>> MISSING_HEADERS = new HashMap<>(); 
	
	private static Map<String,Set<String>> MISSING_KV_PAIR = new HashMap<>(); 

	public static void main(String[] args) {
		
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		try {
			execute();
		} catch (IOException e) {
			
			System.err.println(e);
			
		}		

	}
	public class DataDictionary {
		public String name;
		public String label;
		public Map<String,String> keyValuePair = new HashMap<>();
		private Set<String> BAD_ENTRIES = new HashSet<String>();

		
		public Set<String> ignoreFields = new HashSet<>();
		{
			ignoreFields.add("Value Set: filled value".toUpperCase());
			ignoreFields.add("Value Set: check".toUpperCase());
		}
		
		public DataDictionary(String[] line) {
			this.name = line[0];
			this.label = line[1];
			if(ignoreFields.contains(line[2].trim().toUpperCase())) {
				this.keyValuePair = new HashMap<>();
				this.keyValuePair.put("NOT_ENCODED", "");
			} else {
				this.keyValuePair = buildKV(line, line[2]);
			}
		}

		private Map<String, String> buildKV(String[] line, String denormedKVPairs) {
			Map<String, String> returnSet = new HashMap<>();

			
			denormedKVPairs = denormedKVPairs.replace("Value Set:", "");
			denormedKVPairs = denormedKVPairs.replace("Answer Key:", "");
			denormedKVPairs = denormedKVPairs.replace("LOC_SYMP,  | ", "");
			denormedKVPairs = denormedKVPairs.replace("LOC_PN,", "");
			denormedKVPairs = denormedKVPairs.replace("NO_YES,", "");
			
			String[] strarr = denormedKVPairs.split("\\|");
			
			for(String str: strarr) {
				String[] kv = str.split(",");
				
				if(kv.length < 2 ) {
					
					if(kv.length == 1) {
						String firstchar = kv[0].substring(0,1);
						if(NumberUtils.isCreatable(firstchar)) {
							System.out.println(kv[0]);
						}
					} else {
						System.err.println("Variable " + line[0] + " Invalid format for " + str);
					}
					
					BAD_ENTRIES.add(StringUtils.join(strarr));
					
				} else  {
					
					StringBuilder sb = new StringBuilder();
					
					int x = 0;
					for(String v: kv) {
						if(x==0) {
							x++;
							continue;
						}
						sb.append(v.trim());
						x++;
						if(x != kv.length) sb.append(",");
					}

					if(sb.toString().trim().isEmpty()) continue;
					
					returnSet.put(kv[0].trim(), sb.toString().trim());
					
				}
			}
			
			return returnSet;			
		}
		
	}

	private static void execute() throws IOException {
		Map<String, DataDictionary> dataDict = readDataDictionary(Paths.get(DATA_DIR + /*TRIAL_ID.toUpperCase()*/ "CSSCD" + "-data-dictionary.csv"));
		
		
		decodeDataSet(dataDict);
		//Set<Mapping> set = buildMappings(dataDict,Paths.get(DATA_DIR));
				
		//writeMapping(set);
		
		
	}

	private static void decodeDataSet(Map<String, DataDictionary> dataDict) {
		File dir = new File(DATA_DIR + "encoded_data");
		
		Set<Mapping> mappings = new HashSet<>();
		
		if(dir.isDirectory()) {
			
			File[] files = dir.listFiles();
			
			for(File file: files) {
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
					
					mappings.addAll(buildMappings(dataDict, Paths.get(file.getAbsolutePath())));
					
					List<String[]> linesToWrite = decodeData(dataDict,buffer);
					
					try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + "decoded_data/" + file.getName()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
						for(String[] l: linesToWrite) {
							writer.write(toCsv(l));
						}
					}
					
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		for(Mapping mapping: mappings) {
			
			
			
		}
		
	}

	private static List<String[]> decodeData(Map<String, DataDictionary> dataDict, BufferedReader buffer) throws IOException {
		CSVReader reader = new CSVReader(buffer);

		String[] headersArr = reader.readNext();
		
		String[] line;
		
		List<String[]> linestowrite = new ArrayList<>();
		linestowrite.add(headersArr);
		while((line = reader.readNext()) != null) {
			String[] lineToWrite = new String[line.length];
			int index = 0;
			for(String col: line) {
				String header = headersArr[index];
				
				String value = dataDict.containsKey(header) ? findValue(dataDict,header,col) : col;
				
				lineToWrite[index] = value;
				
				index++;
			}
			linestowrite.add(lineToWrite);
		}
		
		return linestowrite;
	}

	private static String findValue(Map<String, DataDictionary> dataDict, String header, String col) {
		if(col.trim().isEmpty()) return col;
		if(dataDict.containsKey(header)) {
			
			DataDictionary dd = dataDict.get(header);
			if(dd.keyValuePair.containsKey(col)) {
				return dd.keyValuePair.get(col);
			} else {
				if(dd.keyValuePair.containsKey("NOT_ENCODED")) return col;
				if(MISSING_KV_PAIR.containsKey(dd.name)) {
					MISSING_KV_PAIR.get(dd.name).add(col);
				} else {
					Map<String,Set<String>> m = new HashMap<>();
					Set<String> init = new HashSet<String>();
					init.add(col);
					m.put(dd.name, init);
				}
				
				return header + " " + col + " MISSING FROM DATA DICTIONARY!";
				
			}
		} else {
			return header + " " + col + " MISSING FROM DATA DICTIONARY!";
		}
		
	}

	private static void writeMapping(Set<Mapping> mappings) throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mapping.csv"))) {
			
			writer.write(Mapping.printHeader());
			
			for(Mapping mapping: mappings) {
				writer.write(mapping.toCSV() + '\n');
			}
			
		}
		
	}

	private static Set<Mapping> buildMappings(Map<String, DataDictionary> dataDicts, Path path) throws IOException {
		Set<Mapping> mappings = new TreeSet<Mapping>(new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				return o1.getRootNode().compareTo(o2.getRootNode());
			}
		});
		try(BufferedReader buffer = Files.newBufferedReader(path)) {
			CSVReader reader = new CSVReader(buffer);
			
			String[] headersArr = reader.readNext();
			
			List<String> headers = Arrays.asList(headersArr);
			
			Set<String> MissingHeaders = new HashSet<>();
			
			for(String header: headers) {
				
				if(!dataDicts.containsKey(header)) {
					MissingHeaders.add(header);
					continue;
				}
				
				DataDictionary dataDict = dataDicts.get(header);
				
				Mapping mapping = new Mapping();
				
				int colIndex = headers.indexOf(header);
				
				String fileName = path.getFileName().toString();
				
				mapping.setKey(fileName + ':' + colIndex);
				
				String label = dataDict.label;
				
				/*if(label.isEmpty()) {
					
					System.err.println("Data Dict missing label for: " + header + " in data set " + fileName);
					
					continue;
				}*/
				
				
				//label.replaceAll("[^\\x00-\\x7F]", "");
				String conceptPath = 'µ' + ROOT_NODE.replaceAll(PATH_SEPARATOR.toString(), "") + 'µ' + label.trim() + 'µ';
				
				mapping.setRootNode(conceptPath);
				
				//mapping.setDataType(dataDict.type.equalsIgnoreCase("num") ? "TEXT": "TEXT");
				
				mappings.add(mapping);
				
			}
			MISSING_HEADERS.put(path.getFileName().toString(), MissingHeaders);
		}
		
		return mappings;
	}

	private static Map<String,DataDictionary> readDataDictionary(Path dataDict) throws IOException {
		
		Map<String,DataDictionary> dds = new HashMap<>();
		try(BufferedReader buffer = Files.newBufferedReader(dataDict)) {
						
			CSVReader reader = new CSVReader(buffer);
			
			String[] line = reader.readNext();
			
			while((line = reader.readNext()) != null) {
				DataDictionary dd = new BiolinccDataDecoder().new DataDictionary(line);
				dds.put(line[0], dd);
			}
		}
		return dds;
	}

	private static void setClassVariables(String[] args) {
		// TODO Auto-generated method stub
		
	}

}

