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
import java.util.Set;
import java.util.Map.Entry;

import com.opencsv.CSVReader;

public class PHSIdGenerator extends Job {
	
	private static String STUDY_ID_W_ACCESSIONS = "studyid_with_accessions.csv";

	private static String ACCESSIONS_WITH_SOURCE_SUBJECT_COLUMN = "accessions_with_source_subject_column.csv";
	
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
		buildPhsIds();		
	}


	private static void buildPhsIds() throws IOException {
		// check necessary files exist
		if(!Files.exists(Paths.get(DATA_DIR + STUDY_ID_W_ACCESSIONS))) throw new IOException("Missing study_id_w_accesssions file");
		if(!Files.exists(Paths.get(DATA_DIR + ACCESSIONS_WITH_SOURCE_SUBJECT_COLUMN))) throw new IOException("Missing accessions_with_source_subject_columns file");
		
		// Map of study_id and accession with version
		Map<String,String> accessions = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + STUDY_ID_W_ACCESSIONS))){
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] arr = line.split(",");
				
				if(arr.length == 2) {
					accessions.put(arr[0], arr[1]);
				}
				
			}
		}
		// accessions with source_subject_column
		Map<String,Set<String>> accessionsSourceSubjCols = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + ACCESSIONS_WITH_SOURCE_SUBJECT_COLUMN))){
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] arr = line.split("\t");
				
				if(arr.length == 3) {
					
					Set<String> sourceCols = new HashSet<>();
					
					String[] cols = arr[2].split(",");
					
					for(String c: cols) {
						
						sourceCols.add(c.trim());
						
					}
					
					accessionsSourceSubjCols.put(arr[0], sourceCols);
					
				}
				
			}
			
		}
		
		// build dataToWrite or wrap this is buffer writer
		
		Set<String> dataToWrite = new HashSet<>();
		
		for(Entry<String, String> accession: accessions.entrySet()) {
			
			Map<String,String> patientMapping = new HashMap<>();
			
			if(!Files.exists(Paths.get(WRITE_DIR + accession.getKey().toUpperCase() + "_PatientMapping.csv"))) {
				
				System.out.println("Patient Mapping missing in data directory for " + accession.getKey().toUpperCase());
				continue;
				
			};
			
			Map<String,String> idLookup = new HashMap<>();
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + accession.getKey().toUpperCase() + "_PatientMapping.csv"))) {
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] line;
				
				while((line = reader.readNext()) != null) {
			
					idLookup.put(line[0], line[2]);
					
				}
				
			}
			
			File dataDir = new File(DATA_DIR);
			
			if(dataDir.isDirectory()) {
				
				for(String f: dataDir.list()) {
					
					if(f.toLowerCase().contains(accession.getValue().toLowerCase()) && f.toLowerCase().contains("subject.multi")) {
						
						try(BufferedReader buffer = Files.newBufferedReader(Paths.get(new File(DATA_DIR + f).getAbsolutePath()))) {
							
							String phsOnly = accession.getValue().substring(0, 9);
							
							Set<String> srcCols = accessionsSourceSubjCols.get(phsOnly);
							if(srcCols == null) srcCols = new HashSet<>();
							CSVReader reader = new CSVReader(buffer, '\t');
							
							String[] comments;
							
							List<String> headers2 = new ArrayList<String>();

							while((comments = reader.readNext()) != null) {

								boolean isComment = comments[0].toString().startsWith("#") ? true: comments[0].isEmpty() ? true: false;
								
								if(isComment) {
									
									continue;
									
								} else {
									headers2 = Arrays.asList(comments);
									break;
									
								}
								
							}
							
							List<String> headers = new ArrayList<>();
							
							int x = 0;
							Set<Integer> ints = new HashSet<>();
							// make lowercase
							for(String h: headers2) {
								headers.add(x, h.toLowerCase());
								x++;
							}
							
							// see if given headers exist in multi file if not empty the srcCols set
							for(String c: srcCols) {
								if(!headers.contains(c.toLowerCase())) {
									srcCols.remove(c);
								}
							}
							
							// if no source is given use the dbgap_id
							if(srcCols.isEmpty()) {
								
								String[] line;
								

								while((line = reader.readNext()) != null) {
									String[] lineToWrite = null;
									// build a line that will be the data file
									// use the patient mapping to create the first column to be the hpds id that is stored as the value 
									// in patient mapping.
									// line should look like:
									// <HPDS_ID>,phsXXXXXX.vX_<line[srcCol]>
									// use dbgap_id if no source is given
									if(idLookup.containsKey(line[0])) {
										
										String hpdsId = idLookup.get(line[0]);
										
										lineToWrite = new String[] { hpdsId, accession.getValue() + "_" + line[0] };
										dataToWrite.add(toCsv(lineToWrite));
										
									} else {
										
										System.err.println("No dbgap_id found in patient mapping for " + line[0]);
										
									}
									
								}
								
							} else {

								String[] line;
								
								while((line = reader.readNext()) != null) {
									
									String[] lineToWrite = null;

									
									for(String c: srcCols) {
										
										int idx = headers.indexOf(c.toLowerCase());
										
										if(idx == -1) continue;
										
										else {
											
											if(line.length >= idx) {
												
												String sourceId = line[idx];
												
												if(sourceId.isEmpty()) continue;
																								
												else {
													
													if(idLookup.containsKey(line[0])) {
														
														String hpdsId = idLookup.get(line[0]);
														
														lineToWrite = new String[] { hpdsId, accession.getValue() + "_" + sourceId };
														break;

													}
												}
											}
										}
									}
									
									if(lineToWrite != null) {
										
										dataToWrite.add(toCsv(lineToWrite));
										
									} else {
										
										String hpdsId = idLookup.get(line[0]);
										
										lineToWrite = new String[] { hpdsId, accession.getValue() + "_" + line[0] };
										
										dataToWrite.add(toCsv(lineToWrite));
										
									}
								}
							}
						}
						
					}
				}
			}
			
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "AccessionIds.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			for(String str: dataToWrite) writer.write(str);
		}
	}
	
	private static String toCsv(String[] line) {
		StringBuilder sb = new StringBuilder();
		
		int lastNode = line.length - 1;
		int x = 0;
		for(String node: line) {
			
			sb.append('"');
			sb.append(node);
			sb.append('"');
			
			if(x == lastNode) {
				sb.append('\n');
			} else {
				sb.append(',');
			}
			x++;
		}
		
		return sb.toString();
	}
}
