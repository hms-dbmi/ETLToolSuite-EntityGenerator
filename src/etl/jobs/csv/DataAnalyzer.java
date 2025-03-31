package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;
import etl.utils.Utils;

public class DataAnalyzer extends Job {

	public static double NUMERIC_THRESHOLD = .95;
	
	private static Map<Mapping,MappingCounts> MAPPING_COUNTS = new HashMap<Mapping,MappingCounts>();

	public static void main(String[] args) throws Exception {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
		}
		
		execute();
		 
	}

	private static void execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		//TODO we are iterating over all vars in the set like 5 times. We can absolutely reduce this
		
		List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		if(mappings.isEmpty()) System.err.println("NO MAPPINGS FOR " + TRIAL_ID);
		
		List<Mapping> newMappings = analyzeData(mappings);
		
		newMappings.removeAll(mappingsToRemove);
		
		List<Mapping> validatedMappings = validateDataType(newMappings);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			Utils.writeToCsv(buffer, validatedMappings, MAPPING_QUOTED_STRING, MAPPING_DELIMITER);
		}
		
	}

	private static List<Mapping> analyzeData(List<Mapping> mappings) throws IOException {
		List<Mapping> newMappings = new ArrayList<Mapping>();
		
		Map<String,List<Mapping>> mappingsMap = new HashMap<String, List<Mapping>>();
		
		for(Mapping m: mappings) {
			
			String fileName = m.getKey().split(":")[0];
			
			if(mappingsMap.containsKey(fileName)) {
				
				mappingsMap.get(fileName).add(m);
				
			} else {
				System.out.println("Adding file" + fileName + "to file list");
				mappingsMap.put(fileName, new ArrayList<Mapping>(Arrays.asList(m)));
				
			}
			
		}
		
		mappingsMap.entrySet().forEach(entry ->{
			
			//for(Mapping m: entry.getValue()) {
			entry.getValue().parallelStream().forEach(m -> {
				Path path = Paths.get(DATA_DIR + entry.getKey());
				
				if(Files.exists(path)) { 

					try {
						newMappings.add(analyzeData(m,path));
						
					} catch (IOException e) {
						
						e.printStackTrace();
					}
				}
			});
		});
		
		return newMappings;
		
	}
	
	private static List<Mapping> validateDataType(List<Mapping> newMappings) {
		List<Mapping> validatedMapping = new ArrayList<Mapping>();
		newMappings.parallelStream().forEach(mapping -> {
			validatedMapping.add(findDataType(mapping,newMappings));
		});
		String currentDataType = "";
		String currentConcept = "";
		for(Mapping m: validatedMapping) {
			if(m.getRootNode().equals(currentConcept)) {
				if(!m.getDataType().equals(currentDataType)) {
					//TODO the type issues are here - we are just warning of type mismatch not doing anything about it
					//Should add set all mappings that match root node to text on mismatch
					System.out.println(m + " - data type mismatch detected!");				
				}
			} else {
				currentConcept = m.getRootNode();
				currentDataType = m.getDataType();
			}
		}
		return validatedMapping;
	}

	private static Mapping findDataType(Mapping mapping, List<Mapping> newMappings) {
		String conceptPath = mapping.getRootNode();
		
		int totalNumeric = 0;
		int totalAlpha = 0;

		for(Mapping _mapping: newMappings) {
			if(_mapping.getRootNode().equals(conceptPath)) {
				if(MAPPING_COUNTS.containsKey(_mapping)) {
					totalNumeric += MAPPING_COUNTS.get(_mapping).numericVals;
					totalAlpha += MAPPING_COUNTS.get(_mapping).alphaVals;
				} else {
					System.out.println("No numeric counts given for - " + mapping.toString());
				}
			}
		};
		
		
		MappingCounts counts = MAPPING_COUNTS.containsKey(mapping) ? MAPPING_COUNTS.get(mapping) : null;
		
		BigDecimal bd = new BigDecimal(totalNumeric + totalAlpha);
		
		if(counts != null) {
			if(!bd.equals(new BigDecimal(0))) {
				
				BigDecimal percentNumeric = new BigDecimal(totalNumeric).divide(bd, MathContext.DECIMAL128 );
				System.out.println("Numeric Percent: " + percentNumeric);
				//TODO why are we setting numeric threshold to 95% when we already are checking for nulls and nas. What margin of error are we preventing
				BigDecimal numThres = new BigDecimal(NUMERIC_THRESHOLD, MathContext.DECIMAL128 );
				
				//percentNumeric.compareTo(numThres);
				
				if(percentNumeric.compareTo(numThres) >= 0) {
										
					mapping.setDataType("NUMERIC");
					
				} else {
					mapping.setDataType("TEXT");
				}
			}
			
		} else {
			mapping.setDataType("TEXT");
		}
		
		if(mapping.getDataType().equals("NOT_CALCULATED")) {
			System.err.println("ERROR CALCULATING DATA TYPE FOR - " + mapping.toString());
		}
		return mapping;
	}

	private static List<Mapping> mappingsToRemove = new ArrayList<Mapping>();
	
	private class MappingCounts {
		public int numericVals = 0;
		public int alphaVals = 0;
	}
	
	private static Mapping analyzeData(Mapping mapping, Path path) throws IOException {
		
		int col = new Integer(mapping.getKey().split(":")[1]);
				
		try(BufferedReader buffer = Files.newBufferedReader(path)){
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
			
			if(SKIP_HEADERS) csvreader.readNext();

			String[] newLine;
			
			int numericVals = 0;
			int alphaVals = 0;
			MappingCounts counts = new DataAnalyzer().new MappingCounts();
			while((newLine = csvreader.readNext()) != null){

				if(col <= newLine.length - 1) {
					String val = newLine[col];
					
					if(val.isEmpty()) {
						continue;
					}
					//TODO - adjust to encompass NAN and - vals as null OR consider skipping as though empty
					if(NumberUtils.isCreatable(val) || val.equalsIgnoreCase("null") || val.equalsIgnoreCase("na")) {
						numericVals++;
					}
					else {
						alphaVals++;
					}
				}
			}
			
			BigDecimal bd = new BigDecimal(numericVals + alphaVals);
			
			if(!bd.equals(new BigDecimal(0))) {
				mapping.setDataType("NOT_CALCULATED");
			} else {
				System.out.println("No values found! - removing mapping");
				mappingsToRemove.add(mapping);
			}
			counts.numericVals = numericVals;
			counts.alphaVals = alphaVals;
			if(MAPPING_COUNTS.containsKey(mapping)) {
				MAPPING_COUNTS.get(mapping).alphaVals += counts.alphaVals;
				MAPPING_COUNTS.get(mapping).numericVals += counts.numericVals;
			} else {
				MAPPING_COUNTS.put(mapping, counts);
			}
		}
		return mapping;
		
	}

}
