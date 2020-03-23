package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import com.opencsv.CSVReader;

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
					System.out.println(entry.getValue());
					CSVReader csvreader = new CSVReader(reader, ',', '\"', '√');

					String[] line;
					
					while((line = csvreader.readNext()) != null )  {
						if(line[1].contains(entry.getKey())) {
							String rootnode = line[1];
							rootnode = rootnode.replace('µ', '\\');
							line[1] = rootnode;
							if(line[2].equals("null")) continue;
							if(line[3].equals("null")) continue;
							if(!line[2].isEmpty() && line[3].equals("E")) {
								
								line[3] = "";
								
							}
							
							
							buffer.write(toCsv(line));
						}
					}
					
				}
				
			}
			buffer.flush();
			buffer.close();
		}
		
	}
	// filename and rootnode
	private static Map<String, String> sortByRootNodes() throws IOException {
		
		File dataDir = new File(WRITE_DIR);
		// Map of rootNode and trial id
		Map<String,String> rootnodes = new TreeMap<>();
		
		if(dataDir.isDirectory()) {
			
			for(File f: dataDir.listFiles()) {
				
				if(f.getName().contains("_allConcepts")) {
					
					
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(f.getAbsolutePath()))){
						String line;
						String studyid = f.getAbsolutePath();
						while((line = buffer.readLine()) != null) {
							String[] record = line.split(",");
							if(record.length < 1) continue;
							String rootNode = record[1];
							
							rootNode = rootNode.replaceAll("\"", "");
							rootNode = rootNode.substring(1);
							rootNode = rootNode.replaceAll("µ.*", "");
							rootnodes.put(rootNode, studyid);
							break;
						}
						/*
						CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
						String[] line;
						System.out.println(f.getName());
						while((line = reader.readNext()) != null) {
							String rootNode = line[1];
							rootNode = rootNode.substring(1);
							rootNode = rootNode.replace('µ', '\\');
							rootNode = rootNode.replaceAll("\\\\.*", "");
							if(sortedMap.containsKey(rootNode)) continue;
							sortedMap.put(rootNode, f.getAbsolutePath());
						}*/
					}
					
				}
				
			}
			
		}
		return rootnodes;
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
