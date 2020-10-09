package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
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

import org.apache.commons.lang3.ArrayUtils;

import java.util.Map.Entry;

import com.opencsv.CSVReader;

import etl.jobs.Job;
@Deprecated
public class PHSIdGenerator extends Job {
	
	private static String STUDY_ID_W_ACCESSIONS = "studyid_with_accessions.csv";

	private static String ACCESSIONS_WITH_SOURCE_SUBJECT_COLUMN = "accessions_with_source_subject_column.tsv";
	
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
		
		cleanAccessionFile();
		
		buildPhsIds();		
	}


	private static void cleanAccessionFile() throws IOException {
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "AccessionIds.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		writer.close();
	}


	private static void buildPhsIds() throws IOException {
		// check necessary files exist
		if(!Files.exists(Paths.get(DATA_DIR + STUDY_ID_W_ACCESSIONS))) throw new IOException("Missing study_id_w_accesssions file");
		if(!Files.exists(Paths.get(DATA_DIR + ACCESSIONS_WITH_SOURCE_SUBJECT_COLUMN))) throw new IOException("Missing accessions_with_source_subject_columns file");
		
		// Map of study_id and accession with version
		Map<String,String> accessions = new HashMap<>();
		Map<String,String> topmedAccessions = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + STUDY_ID_W_ACCESSIONS))){
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] arr;
			
			while((arr = reader.readNext()) != null) {
				
				if(arr.length == 3) {
				
					if(arr[2].isEmpty()) {
					
						accessions.put(arr[0].toUpperCase(), arr[1]);
					
					} else {
						
						if(!arr[1].trim().isEmpty()) {
						
							accessions.put(arr[0].toUpperCase(), arr[1]);
						
						} else {
							
							accessions.put(arr[0].toUpperCase(), arr[2]);
							
						}
							
						topmedAccessions.put(arr[0].toUpperCase(), arr[2]);
						
					}
				
				}
				
			}
		}
		// accessions with source_subject_column
		
		
		// build dataToWrite or wrap this is buffer writer
		
		ArrayList<String> parentAccesionsdToWrite = buildAccessions(accessions);
		
		ArrayList<String> topMedAccesionsdToWrite = buildAccessions(topmedAccessions);
		
		if(parentAccesionsdToWrite.isEmpty()) {
			System.out.println("No Parent Accessions found to write");
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "ParentAccessionIds.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			for(String str: parentAccesionsdToWrite) writer.write(str);
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "AccessionIds.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			for(String str: parentAccesionsdToWrite) writer.write(str);
		}
		if(topMedAccesionsdToWrite.isEmpty()) {
			System.out.println("No Topmed Accessions found to write");
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "TopmedAccessionIds.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			for(String str: topMedAccesionsdToWrite) writer.write(str);
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "AccessionIds.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			for(String str: topMedAccesionsdToWrite) writer.write(str);
		}
		
		
				/*
				for(String f: dataDir.list()) {
					
					
					
					if(f.toLowerCase().contains(accession.getValue().toLowerCase()) && f.toLowerCase().contains("subject.multi")) {
						
						System.out.println("found!");
						
						System.out.println(f);
						
						
						
						doesSubjectMultiExist = true;
						
						try(BufferedReader buffer = Files.newBufferedReader(Paths.get(new File(DATA_DIR + f).getAbsolutePath()))) {
							
							String phsOnly = accession.getValue().substring(0, 9);
							
							String srcCol = accessionsSourceSubjCols.get(phsOnly);
							
							if(srcCol == null || srcCol.isEmpty()) {
								System.out.println("error source column not given for " + phsOnly);
								// goto next study
								continue;
							}
							
							CSVReader reader = new CSVReader(buffer, '\t');
							
							String[] headers;
							
							//////////
							System.out.println(phsOnly);
							
							while((headers = reader.readNext()) != null) {

								boolean isComment = headers[0].toString().startsWith("#") ? true: headers[0].trim().isEmpty() ? true: false;
								
								if(isComment) {
									
									continue;
									
								} else {
									
									break;
									
								}
								
							}
							
						
							int headeridx = ArrayUtils.indexOf(headers, srcCol);
							
							if(headeridx == ArrayUtils.INDEX_NOT_FOUND) {
								System.err.println("Header column not found for " + phsOnly + " using " + srcCol);
							}
							// if no source is given use the dbgap_id
							

							String[] line;
							
							while((line = reader.readNext()) != null) {
								
								String[] lineToWrite = null;
										
								if(line.length >= headeridx) {
									
									String sourceId = line[headeridx];
									
									if(sourceId.isEmpty()) {
										System.out.println(phsOnly + " missing source subject id for: " + line);
										continue;
									}
																					
									else {
										
										if(idLookup.containsKey(line[0])) {
											
											String hpdsId = idLookup.get(line[0]);
											String phs = topmedAccessions.containsKey(accession.getKey().toUpperCase()) ? topmedAccessions.get(accession.getKey().toUpperCase()) : accession.getValue();
											lineToWrite = new String[] { hpdsId, phs + "_" + sourceId };

										} else {
											System.out.println("missing accession for " + line[0]);
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
						
						System.out.println(dataToWrite.size());

					}*/
	
		
	}
	
	private static ArrayList<String> buildAccessions(Map<String, String> accessions) throws IOException {
		ArrayList<String> dataToWrite = new ArrayList<>();
		
		Map<String,String> accessionsSourceSubjCols = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + ACCESSIONS_WITH_SOURCE_SUBJECT_COLUMN))){
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] arr = line.split("\t");
				
				accessionsSourceSubjCols.put(arr[0], arr[1]);
				
			}
			
		}
		
		for(Entry<String, String> accession: accessions.entrySet()) {
			
			Map<String,String> patientMapping = new HashMap<>();
			
			if(!Files.exists(Paths.get(DATA_DIR + accession.getKey().toUpperCase() + "_PatientMapping.v2.csv"))) {
				
				System.out.println("Patient Mapping missing in data directory for " + accession.getKey().toUpperCase());
				continue;
				
			} else {
				
				System.out.println(DATA_DIR + accession.getKey().toUpperCase() + "_PatientMapping.v2.csv found.  Building Identifiers." );
				
				
			};
			
			Map<String,String> idLookup = new HashMap<>();
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + accession.getKey().toUpperCase() + "_PatientMapping.v2.csv"))) {
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] line;
				
				while((line = reader.readNext()) != null) {
			
					idLookup.put(line[0], line[2]);
					
				}
				
			}
			
			File dataDir = new File(DATA_DIR);
			
			if(dataDir.isDirectory()) {
								
				File[] files = dataDir.listFiles(new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						System.out.println(name.toLowerCase());
						System.out.println(accession.getValue().toLowerCase());

						if(name.toLowerCase().contains(accession.getValue().toLowerCase()) && name.toLowerCase().contains("subject.multi") && name.endsWith("txt")) {
							return true;
						}
						return false;
					}
				});
				
				if(files.length != -1 ) {
					
					for(File file: files) {
						
						try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
						
							CSVReader reader = new CSVReader(buffer, '\t');
							
							String[] headers;
							
							String phsOnly = accession.getValue().substring(0, 9);
							
							String srcCol = accessionsSourceSubjCols.get(phsOnly);
							
							if(srcCol == null || srcCol.isEmpty()) {
								System.out.println("error source column not given for " + phsOnly);
								// goto next study
								continue;
							}
							//////////
							System.out.println(phsOnly);
							
							while((headers = reader.readNext()) != null) {

								boolean isComment = headers[0].toString().startsWith("#") ? true: headers[0].trim().isEmpty() ? true: false;
								
								if(isComment) {
									
									continue;
									
								} else {
									
									break;
									
								}
								
							}
							int headeridx = ArrayUtils.indexOf(headers, srcCol);
							
							if(headeridx == ArrayUtils.INDEX_NOT_FOUND) {
								System.err.println("Header column not found for " + phsOnly + " using " + srcCol);
							}
							
							String[] line;
							
							while((line = reader.readNext()) != null) {
								
								String[] lineToWrite = null;
										
								if(line.length >= headeridx) {
									
									String sourceId = line[headeridx];
									
									if(sourceId.isEmpty()) {
										System.out.println(phsOnly + " missing source subject id for: " + line);
										continue;
									}
																					
									else {
										
										if(idLookup.containsKey(line[0])) {
											
											String hpdsId = idLookup.get(line[0]);
											String phs = accessions.containsKey(accession.getKey().toUpperCase()) ? accessions.get(accession.getKey().toUpperCase()) : accession.getValue();
											lineToWrite = new String[] { hpdsId, phs + "_" + sourceId };

										} else {
											System.out.println("missing accession for " + line[0]);
										}
									}
								
								}
								
								if(lineToWrite != null) {
									
									dataToWrite.add(new String(toCsv(lineToWrite)));
									
								} 
							}
						}
					}
					
				} else {
					
					System.err.println("Missing subject.multi file for " + accession.getKey() + " - " + accession.getValue());
					
				}
			}
		}
		return dataToWrite;
	}
}
