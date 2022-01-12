package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import java.util.Map.Entry;
import java.util.stream.Stream;

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

	public static double NUMERIC_THRESHOLD = .99;
	
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
		
		List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		if(mappings.isEmpty()) System.err.println("NO MAPPINGS FOR " + TRIAL_ID);
		
		List<Mapping> newMappings = analyzeData(mappings);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "redcoralmapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			Utils.writeToCsv(buffer, newMappings, MAPPING_QUOTED_STRING, MAPPING_DELIMITER);
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
				
				mappingsMap.put(fileName, new ArrayList<Mapping>(Arrays.asList(m)));
				
			}
			
		}
		
		mappingsMap.entrySet().forEach(entry ->{
			
			//for(Mapping m: entry.getValue()) {
			entry.getValue().parallelStream().forEach(m -> {
				Path path = Paths.get(DATA_DIR + File.separatorChar + entry.getKey());
				
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

	private static Mapping analyzeData(Mapping mapping, Path path) throws IOException {
		
		int col = new Integer(mapping.getKey().split(":")[1]);
		System.out.println(mapping);
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
			
			while((newLine = csvreader.readNext()) != null){
				
				if(col < newLine.length - 1) {
				
					String val = newLine[col];
					if(val.isEmpty()) {
						continue;
					}
					if(NumberUtils.isCreatable(val)) {
						numericVals++;
					} else if(val != null || !val.isEmpty()){
						alphaVals++;
					};
				}
			}
			
			BigDecimal bd = new BigDecimal(numericVals + alphaVals);
			
			if(!bd.equals(new BigDecimal(0))) {
				
				BigDecimal percentNumeric = new BigDecimal(numericVals).divide(bd, MathContext.DECIMAL128 );
				
				BigDecimal numThres = new BigDecimal(NUMERIC_THRESHOLD, MathContext.DECIMAL128 );
				
				//percentNumeric.compareTo(numThres);
				
				if(percentNumeric.compareTo(numThres) >= 0) {
										
					mapping.setDataType("NUMERIC");
					
				} else {
					mapping.setDataType("TEXT");
				}
			}
		}
		return mapping;
		
	}

}
