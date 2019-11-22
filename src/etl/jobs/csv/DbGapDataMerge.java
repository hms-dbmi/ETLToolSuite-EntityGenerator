package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

public class DbGapDataMerge extends Job {

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
		
		Map<String, String> rootNodeSort = sortByRootNodes();

		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "MergedAllConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(Entry<String,String> entry: rootNodeSort.entrySet()) {
				
				try(BufferedReader reader = Files.newBufferedReader(Paths.get(entry.getValue()))){
					
					CSVReader csvreader = new CSVReader(reader, ',', '\"', '√');

					String[] line;
					while((line = csvreader.readNext()) != null )  {
						buffer.write(toCsv(line));
					}
					
				}
				
			}
			buffer.flush();
			buffer.close();
		}
		
	}
	// filename and rootnode
	private static Map<String, String> sortByRootNodes() throws IOException {
		File dataDir = new File(DATA_DIR);
		Map<String, String> sortedMap = new TreeMap<String, String>();
		if(dataDir.isDirectory()) {
			
			for(File f: dataDir.listFiles()) {
				
				if(f.getName().contains("allConcepts")) {
					
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(f.getAbsolutePath()))){
						
						CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
						String[] line;
						
						while((line = reader.readNext()) != null) {
							String rootNode = line[1];
							rootNode = rootNode.substring(1);
							rootNode = rootNode.replaceAll("\\\\.*", "");
							sortedMap.put(rootNode, f.getAbsolutePath());
							
						}
						
					}
					
				}
				
			}
			
		}
		return sortedMap;
	}
	private static char[] toCsv(String[] strings) {
		StringBuilder sb = new StringBuilder();
		
		int x = strings.length;
		
		for(String str: strings) {
			sb.append('"');
			sb.append(str);
			sb.append('"');
			if(x != 1 ) {
				sb.append(',');
			}
			x--;
		}
		sb.append('\n');
		return sb.toString().toCharArray();
	}
}
