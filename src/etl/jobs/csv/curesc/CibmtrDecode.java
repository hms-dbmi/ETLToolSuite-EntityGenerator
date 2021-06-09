package etl.jobs.csv.curesc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import com.opencsv.CSVReader;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

public class CibmtrDecode extends Job {

	public static void main(String[] args) {
		
		try {
			
			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}
		
		try {
			
			execute();
			
		} catch (IOException  e) {
			
			System.err.println(e);
			
		}
	}

	private static void execute() throws IOException {
		Map<String, String> dataDict = buildDataDict();
		
		decode(dataDict);
		
		buildMappingFile();
	}

	private static void buildMappingFile() throws IOException {
				
		Map<String,String> conceptpaths = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./data/cureSC_data_dictionary.csv"))){
			
			CSVReader reader = new CSVReader(buffer);
			
			reader.readNext();
			
			String[] line;
			
			while((line = reader.readNext()) != null ) {
				
				conceptpaths.put(line[0].toUpperCase() , 'µ' + "Hematopoietic Cell Transplant for Sickle Cell Disease (HCT for SCD) ( phs002385 )" +'µ' + line[3] + 'µ' + line[4] + 'µ');
				
			}
			
		}
		List<Mapping> mappings = new ArrayList<Mapping>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./data/curesc_year2_v2.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			String[] headers = reader.readNext();
			
			for(String header: headers) {
				if(conceptpaths.containsKey(header)) {
					int index = ArrayUtils.indexOf(headers, header);
					Mapping mapping = new Mapping();
					mapping.setDataType("TEXT");
					mapping.setKey("curesc_decoded.csv:" + index);
					mapping.setRootNode(conceptpaths.get(header));
					mappings.add(mapping);
				} else {
					System.err.println("Missing path for column " + header);
				}
			}
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + "mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			for(Mapping mapping: mappings) {
				writer.write(mapping.toCSV() + '\n');
			}
		}
	}

	private static void decode(Map<String, String> dataDict) throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./data/curesc_year2_v2.csv"))) {
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("./data/curesc_decoded.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] header = reader.readNext();
				
				writer.write(toCsv(header));
				
				String[] line;
				
				while((line = reader.readNext()) != null ) {
					int x = 0;
					String[] linetowrite = new String[line.length];
					for(String cell: line) {
						String newCell = cell.replaceAll("\\.0", "");
						String key = header[x].toLowerCase() + ':' + newCell;
						
						if(!cell.isEmpty() && dataDict.containsKey(key)) {
							newCell = dataDict.get(key);
							linetowrite[x] = newCell;
						} else {
							linetowrite[x] = cell;
						}
						
						x++;
					}
					writer.write(toCsv(linetowrite));
				}
				
			}
			
		}		
	}

	private static Map<String, String> buildDataDict() throws IOException {
		Map<String, String> dataDict = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get("./data/cureSC_data_dictionary.csv"))){
			
			CSVReader reader = new CSVReader(buffer);
			
			reader.readNext();
			
			String[] line;
			
			while((line = reader.readNext()) != null ) {
				
				dataDict.put(line[0] + ':' + line[1], line[2]);
				
			}
			
		}
		
		return dataDict;
	}
	

}
