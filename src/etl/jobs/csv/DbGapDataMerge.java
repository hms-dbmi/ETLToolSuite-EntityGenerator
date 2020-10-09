package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;

import etl.jobs.Job;

import java.util.TreeMap;

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

	private static void execute() throws IOException, InterruptedException {
		
		Map<String, String> rootNodeSort = sortByRootNodes();
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			buffer.close();
		}
			
		for(Entry<String,String> entry: rootNodeSort.entrySet()) {
			System.out.println("merging " + entry.getKey());
			ProcessBuilder processBuilder = new ProcessBuilder();
			
			processBuilder.command("bash", "-c", "cat " + entry.getValue() + " >> " + WRITE_DIR + "allConcepts.csv");
			
			Process process = processBuilder.start();
			
			StringBuilder output = new StringBuilder();
			
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(process.getInputStream()));

			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}

			int exitVal = process.waitFor();
			if (exitVal == 0) {
				System.out.print(entry.getKey() + " merge Success!");
				System.out.println(output);
			} else {
				System.out.println(exitVal);
				System.err.print(entry.getKey() + " merge unsuccessful!");
			}		
			
		}
		
	}
	// filename and rootnode
	private static Map<String, String> sortByRootNodes() throws IOException {
		
		File dataDir = new File(DATA_DIR);
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

}
