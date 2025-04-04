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
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

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
		System.out.println("Processing raw data files");
		processRawData();
		
		generateMappingFile();
	}

	private static void generateMappingFile() throws JsonParseException, JsonMappingException, IOException {
		File dataDir = new File(DATA_DIR);
		Set<Mapping> mappings = new HashSet<>();
		if(dataDir.isDirectory()) {
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

	private static void processRawData() throws IOException {
		File processingDir = new File(PROCESSING_FOLDER);
		
		if(processingDir.isDirectory()) {
			for(File f: processingDir.listFiles()) {
				if(!f.getName().endsWith("eav.txt")) continue;
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(f.getAbsolutePath()))) {
					
					System.out.println("processing " + f.getName());
					TreeMap<String,Map<String,String>> decodeLookup = decodeLookup(f);

					//skip header
					String[] headers = buffer.readLine().split("\t");
					
					for(String h: headers) {
						if(h.equalsIgnoreCase("variable")) {
							break;
						}
					}
					String linea;
					
					Map<String,List<String[]>> linesToWrite = new HashMap<>();
					while((linea = buffer.readLine())!=null ) {
						String[] line = linea.split("\t");
						String varName = line[line.length - 2];
						String varValueRaw = line[line.length - 1];
						String valueToWrite = varValueRaw;
						if(decodeLookup.containsKey(varName)) {
							// is encoded 
							valueToWrite = lookupDecodedValue(varValueRaw,decodeLookup.get(varName));
							if(valueToWrite.equals(varValueRaw)) {
								System.err.println("no decoding mapping found for " + f.getName() + " " + varValueRaw);
							}
						}
						String[] lineToWrite = { line[1], valueToWrite + '\n'};
						
						if(linesToWrite.containsKey(varName)) {
							linesToWrite.get(varName).add(lineToWrite);
						} else {
							linesToWrite.put(varName, new ArrayList<String[]>());
							linesToWrite.get(varName).add(lineToWrite);
						}
						
					}
					for(Entry<String,List<String[]>> outputData: linesToWrite.entrySet()) {
						try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + f.getName() + "." + outputData.getKey()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
							
							for(String[] record: outputData.getValue()) {
								writer.write(arrToCsv(record));
							}
							writer.flush();
							writer.close();
						}
					}
				}
			}
		
		
		}
		
	}

	private static String lookupDecodedValue(String varValueRaw, Map<String, String> map) {
		if(map.containsKey(varValueRaw)) {
			return map.get(varValueRaw);
		} 
		return varValueRaw;
	}

	private static TreeMap<String, Map<String,String>> decodeLookup(File file) {
		System.out.println("building decode lookup - " + file.getName());
		String fileNameNoExt = file.getName().split("\\.")[0];
		
		String fileNameDD = PROCESSING_FOLDER + fileNameNoExt.replace("_eav", "") + "_variable_DD.txt";
		TreeMap<String,Map<String,String>> decodeLookup = new TreeMap<String, Map<String,String>>();

		System.out.println("looking for data dictionary " + fileNameDD);
		if(Files.exists(Paths.get(fileNameDD).toAbsolutePath())) {
			// return var
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(fileNameDD))) {
				buffer.readLine(); // skip header
				String line;
				while((line = buffer.readLine()) != null) {
					if(line.trim().isEmpty()) continue;
					String[] lineArr = line.split("\t"); // we know dictionaries are deliimited by tabs
					// if type encoded populate decodedLookup
					// type is in the third column of the data dictionary
					String varName = lineArr[0];
					TreeMap<String,String> decodeMapping = new TreeMap<>();
					
					if(lineArr[2].toLowerCase().equals("encoded")) {
						// values list is located in column 9+ need to iterate over the length
						// of the lineArr by column 9+
						int index = 8;
						
						if(lineArr.length >= 9) {
							while(index < lineArr.length) {
								String mappingDenorm = lineArr[index];
								String[] mappingNorm = mappingDenorm.split("=");
								if(mappingNorm.length == 2) {
									decodeMapping.put(mappingNorm[0], mappingNorm[1]);
								} else {
									System.err.println("bad encoding found for " + fileNameDD + " - "+ Arrays.toString(lineArr));

								}
								index++;
							}
						} else {
							System.err.println("no encoded variable found for " + fileNameDD + " - "+ Arrays.toString(lineArr));
						}
						if(!decodeLookup.containsKey(varName)) {
							decodeLookup.put(varName, decodeMapping);
						} else {
							// duplicate decoding lines dectected in data dictionary doesn't make sense report as error and remove
							// will need further curation to make it viable
							System.err.println("Duplicate encodings found for " + fileNameDD + " - "+ Arrays.toString(lineArr));
							decodeLookup.remove(varName);
						}
					}
					
				}
				
			} catch ( Exception e) {
				
				e.printStackTrace();
			}
		} else {
			System.err.println(fileNameDD + " Does not exist in the " + PROCESSING_FOLDER);
		}
		return decodeLookup;
	}

}
