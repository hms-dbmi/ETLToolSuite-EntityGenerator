package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

public class AccessionFix extends Job {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		Map<String,String> map = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "HRMN_HPDS_ID_MAPPING.csv"))) {
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();			
			String[] line;
			
			while((line = csvreader.readNext()) != null) {
				
				//set.add(line[2].substring(0, 9));
				map.put(line[0], line[1]);
			}
			
		}
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "consents.csv"))) {
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();			
			String[] line;
			
			Set<String> set = new HashSet<>();
			
			while((line = csvreader.readNext()) != null) {
				if(map.containsKey(line[0])) continue;
				//set.add(line[2].substring(0, 9));
				String phsId = lookupPhsId(line[2].substring(0,9));
				
				
			}
			System.out.println(set.size());
		}		
	}

	private static String lookupPhsId(String lookupval) throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "consents.csv"))) {
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();			
			String[] line;
			
			while((line = csvreader.readNext()) != null) {
				String phs = line[1].substring(0,9);
				//set.add(line[2].substring(0, 9));
				if(phs.equals(lookupval)) return line[1];
				
			}
		}
		return null;
	}

}
