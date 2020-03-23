package etl.jobs.csv;

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

public class BDCVariableCollector extends Job {

	private static List<String> malformedFiles = new ArrayList<String>();

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
			
		} catch (IOException | InterruptedException e) {
			
			System.err.println(e);
			
		}
		
	}

	private static void execute() throws IOException, InterruptedException {
		
		runVariableAndHeaderCount();
		
		checkForEmptyVariables();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Malformed Data Files.txt"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(String malformed: malformedFiles) {
				writer.write(malformed + '\n');
				writer.flush();
			}
			
		}
	}

	private static void checkForEmptyVariables() throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Empty_Variables.txt"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
		}
		File dataDir = new File(DATA_DIR);
		Map<String, List<String>> fileAndVariables = new HashMap<String,List<String>>();
		Map<String, List<String>> fileAndHeaders = new HashMap<String,List<String>>();

		if(dataDir.isDirectory()) {
			
			int totalVariablesWithValues = 0;
			int malformedrecords = 0;
			for(File currentFile: dataDir.listFiles()) {
				if(currentFile.getName().contains("phs")) {
					
					String pht = currentFile.getName().split("\\.")[2];
					String[] headers = new String[0];
					Map<String,List<String>> colWValues = new HashMap<>();
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(currentFile.getAbsolutePath()))){
						String line;
						
						while((line = buffer.readLine()) != null) {
							
							String[] record = line.split("\t");
							// skip 
							if(record[0] == null) continue;
							if(record[0].isEmpty()) continue;
							if(record[0].contains("#")) continue;
							if(record[0].equalsIgnoreCase("dbgap_subject_id")) {
								headers = record;
								break;
							}
							
						}
					}
					
					int totalLines = 0;
					int currentMalformed = 0;
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(currentFile.getAbsolutePath()))){
						String line;
						colWValues = new HashMap<>();
						
						for(String header: headers) {
							colWValues.put(header, new ArrayList<String>());
						}
						
						while((line = buffer.readLine()) != null) {
							
							String[] record = line.split("\t");
							// skip 
							if(record[0] == null) {
								continue;
							}
							if(record[0].contains("#")) continue;
							if(record[0].isEmpty()) {
								continue;
							}
							if(record[0].equalsIgnoreCase("dbgap_subject_id")) continue;
							totalLines++;
							if(record.length != headers.length) {
								currentMalformed++;
								malformedrecords++;
								continue;
								
							}
							int idx = 0;
							for(String cell: record) {
								if(cell.trim().isEmpty()) {
									idx++;

									continue;
								}
								if(cell.equalsIgnoreCase("null")) {
									idx++;

									continue;
								}
								if(!colWValues.containsKey(headers[idx])) {
									colWValues.put(headers[idx], new ArrayList<String>(Arrays.asList(cell)));
								} else {
									colWValues.get(headers[idx]).add(cell);
								}
								idx++;
							}
						}
					}
					
					if(totalLines == currentMalformed) {
						malformedFiles.add(currentFile.getName());
						continue;
					}
					try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Empty_Variables.txt"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
						for(Entry<String, List<String>> entry: colWValues.entrySet()) {
							
							if(entry.getValue().isEmpty()) {
								
								totalVariablesWithValues++;
								writer.write(currentFile.getName() + " " + entry.getKey() + '\n');
							}
							
						}
					}
				}	
			}
			
			System.out.println("Total Variables without values:");
			System.out.println(totalVariablesWithValues);
			System.out.println("");
			System.out.println("Total Malformed records:");
			System.out.println(malformedrecords);
		}
	}

	private static void runVariableAndHeaderCount() throws IOException {
		File dataDir = new File(DATA_DIR);
		Map<String, List<String>> fileAndVariables = new HashMap<String,List<String>>();
		Map<String, List<String>> fileAndHeaders = new HashMap<String,List<String>>();

		if(dataDir.isDirectory()) {
			
			for(File currentFile: dataDir.listFiles()) {
				
				if(currentFile.getName().contains("phs")) {
					
					String pht = currentFile.getName().split("\\.")[2];
					  
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(currentFile.getAbsolutePath()))){
						
						String line;
						
						while((line = buffer.readLine()) != null) {
							
							String[] record = line.split("\t");
							// skip 
							if(record[0] == null) continue;
							if(record[0].isEmpty()) continue;
							if(record[0].contains("##")) {
								if(line.contains("phv")) {
									
									for(String cell: record) {
										
										if(cell.contains("phv")) {
											if(!fileAndVariables.containsKey(pht)) {
												cell = cell.split("\\.")[0];
												fileAndVariables.put(pht, new ArrayList<String>(Arrays.asList(cell)));
											} else {
												cell = cell.split("\\.")[0];
												if(fileAndVariables.get(pht).contains(cell)) continue;
												fileAndVariables.get(pht).add(cell);
											}
										}
									}
									
								} else {
									continue;
								}
								// collect headers in next row
								line = buffer.readLine();
								record = line.split("\t");
								
								for(String cell: record) {
									
									//if(cell.equalsIgnoreCase("dbgap_subject_id")) continue;
									//if(cell.equalsIgnoreCase("dbgap_sample_id")) continue;
									//if(cell.equalsIgnoreCase("BioSample Accession")) continue;
									if(!fileAndHeaders.containsKey(pht)) {
										cell = cell.split("\\.")[0];

										fileAndHeaders.put(pht, new ArrayList<String>(Arrays.asList(cell)));
									} else {
										cell = cell.split("\\.")[0];
										
										if(fileAndHeaders.get(pht).contains(cell)) continue;

										fileAndHeaders.get(pht).add(cell);
									}
									
								}
								break;
							};
							
						}
						
					}
					
				}
				
			}
			
		}
		
		int phvtotal = 0;
		int headertotal = 0;
		for(Entry<String,List<String>> entry: fileAndVariables.entrySet()) {
			
			System.out.println("Total Variables in raw data");
			System.out.println(entry.getKey() + "=" + entry.getValue().size());
			System.out.println("");
			phvtotal += entry.getValue().size();
			
			System.out.println("Total Headers in raw data");
			System.out.println(entry.getKey() + "=" + fileAndHeaders.get(entry.getKey()).size());
			System.out.println("");
			headertotal += fileAndHeaders.get(entry.getKey()).size();
						
		}
		
		System.out.println("Distinct Variables Grand total:");
		System.out.println(phvtotal);
		
		System.out.println("Distinct Headers Grand total:");
		System.out.println(headertotal);
		
		
		Set<String> distinctVars = new HashSet<>();
		
		for(Entry<String,List<String>> entry: fileAndVariables.entrySet()) {
			distinctVars.addAll(entry.getValue());
			
		}
		System.out.println("Distinct Variables Grand total:");
		System.out.println(distinctVars.size());
		
		
		distinctVars = new HashSet<>();
		
		for(Entry<String,List<String>> entry: fileAndHeaders.entrySet()) {
			distinctVars.addAll(entry.getValue());
			
		}
		System.out.println("Distinct Header Grand total:");
		System.out.println(distinctVars.size());
				
	}

}
