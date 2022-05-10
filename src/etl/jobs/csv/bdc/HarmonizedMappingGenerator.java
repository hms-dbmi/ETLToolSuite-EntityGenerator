package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import etl.jobs.mappings.Mapping;

public class HarmonizedMappingGenerator extends BDCJob {

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

	private static void execute() throws IOException {

		// process raw data to be 1 variable per file
		processRawData();
		
		generateMappingFile();
	}

	private static void generateMappingFile() throws JsonParseException, JsonMappingException, IOException {
		File dataDir = new File(DATA_DIR);
		Set<Mapping> mappings = new HashSet<>();
		if(dataDir.isDirectory()) {
			Map<String,String> variableGroup = getVariableSubGroups();
			
			for(File f: dataDir.listFiles()) {
				if(f.getName().endsWith("json")) continue;
				String fname = f.getName();
				
				String subgroup = fname.replace("topmed_dcc_harmonized_", "").replaceAll("\\_v[0-9]_.*", "");
				
				String varname = fname.split("\\.")[fname.split("\\.").length - 1];
				
				//System.out.println(varname);
				Mapping m = new Mapping();
				m.setKey(f.getName() + ":1");
				m.setRootNode(PATH_SEPARATOR + "DCC Harmonized data set" + PATH_SEPARATOR + 
						subgroup + PATH_SEPARATOR +
						varname + PATH_SEPARATOR);
				m.setDataType("TEXT");
				mappings.add(m);
			
			}
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mapping_new_search.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Mapping m: mappings) {
				buffer.write(m.toCSV()+'\n');
			}
		}
	}

	private static Map<String, String> getVariableSubGroups() throws JsonParseException, JsonMappingException, IOException {
		File dictDir = new File(DICT_DIR);
		Map<String,String> subGroups = new HashMap<>();
		if(dictDir.isDirectory()) {
			for(File f: dictDir.listFiles()) {
				if(f.getName().endsWith("json")) {
					ObjectMapper mapper = new ObjectMapper();
					Map<?, ?> map = mapper.readValue(f, Map.class);

					String subGroupName = map.get("phenotype_concept").toString();
					String varName = map.get("name").toString();
					
					subGroups.put(varName, subGroupName);
					
					Map<Object,Object> updateVarName = new HashMap<>();
					updateVarName.putAll(map);
					updateVarName.put("var_name", varName);
					updateVarName.remove("name");
					
					mapper.writeValue(Paths.get(WRITE_DIR + f.getName()).toFile(), updateVarName);
					
				}
			}
		}
		Arrays.asList(subGroups);
		return subGroups;
	}

	private static void processRawData() throws IOException {
		File processingDir = new File(PROCESSING_FOLDER);
		
		if(processingDir.isDirectory()) {
			for(File f: processingDir.listFiles()) {
				
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(f.getAbsolutePath()))) {
					//skip header
					String[] headers = buffer.readLine().split("\t");
					
					int varColumn = 0;
					for(String h: headers) {
						if(h.equalsIgnoreCase("variable")) {
							break;
						} else {
							varColumn++;
						}
					}
					String linea;
					
					Map<String,List<String[]>> linesToWrite = new HashMap<>();
					String variableName = null;
					
					while((linea = buffer.readLine())!=null ) {
						String[] line = linea.split("\t");
						String varName = line[6];
						
						String[] lineToWrite = { line[1], line[line.length - 1]};
						
						if(linesToWrite.containsKey(varName)) {
							linesToWrite.get(varName).add(lineToWrite);
						} else {
							linesToWrite.put(varName, new ArrayList<String[]>());
							linesToWrite.get(varName).add(lineToWrite);
						}
						
					}
					for(Entry<String,List<String[]>> outputData: linesToWrite.entrySet()) {
						try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + f.getName() + "." + outputData.getKey()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
							String[] headersToWrite = { "unique_subject_key", outputData.getKey()};
							//ewriter.write(toCsv(headersToWrite));
							
							for(String[] record: outputData.getValue()) {
								writer.write(toCsv(record));
							}
							writer.flush();
							writer.close();
						}
					}
				}
			}
		
		
		}
		
	}

}
