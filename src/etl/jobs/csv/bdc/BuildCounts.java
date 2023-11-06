package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

public class BuildCounts extends BDCJob {

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
		Map<String,Set<String>> map = buildCountsMap();
		
		writeCounts(map);
		
	}

	private static void writeCounts(Map<String, Set<String>> map) throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "concept_counts.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)){
			for(Entry<String,Set<String>> entry: map.entrySet()) {
				writer.write(entry.getKey() + "," + new Integer(entry.getValue().size() ).toString() + '\n' );
			}
		}
		
	}

	private static Map<String, Set<String>> buildCountsMap() throws IOException {
		Map<String,Set<String>> map = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "allConcepts2.csv"))) {
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader reader = builder.build();
			
			String[] line;
			while((line = reader.readNext()) != null) {
				
				String[] conceptPath = line[1].split("\\\\");
				if(conceptPath.length < 3 ) continue;
				if(map.containsKey(conceptPath[1])) {
					map.get(conceptPath[1]).add(line[1]);
				} else {
					Set<String> set = new HashSet<String>();
					set.add(line[1]);
					map.put(conceptPath[1], set);
				}
				
				
			}
		}
		
		return map;
	}

}
