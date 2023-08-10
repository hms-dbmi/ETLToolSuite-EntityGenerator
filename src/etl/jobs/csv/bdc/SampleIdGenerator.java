package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.Mapping;

public class SampleIdGenerator extends BDCJob {

	private static List<String> SAMPLE_HEADERS = new ArrayList<String>();
	static {
		
		SAMPLE_HEADERS.add("SAMPLE_ID");
		SAMPLE_HEADERS.add("SAMPLEID");
		SAMPLE_HEADERS.add("SAMPID");
		SAMPLE_HEADERS.add("SAMP_ID");

	}
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
		//cleanSampleIdFile();

		buildSampleIds();
		
		
	}

	private static void buildSampleIds() throws IOException {
		
		File dataDir = new File(DATA_DIR);
		
		List<String> patientNums = new ArrayList<>();
		List<String> sampleIds = new ArrayList<>();
		List<Mapping> sampleIdMappings = new ArrayList<>();
		Map<String,Integer> sampleIdCountPerStudy = new HashMap<>();
		
		if(dataDir.isDirectory()) {
			
			for(String f: dataDir.list()) {
									
				if(f.toLowerCase().contains("sample.multi")) {
					
					String phsOnly = f.substring(0, 9);
					
					String studyId = getStudyAccessions(phsOnly);
					
					if(studyId == null || studyId.isEmpty()) {
						System.err.println("No study associated with " + phsOnly + " in managed input file");
						continue;
					};
					
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(new File(DATA_DIR + f).getAbsolutePath()))) {
						
						Map<String,String> patientNumLookup = getPatientMapping(studyId);
						
						List<String> headers2 = new ArrayList<String>();
						
						CSVReader reader = new CSVReader(buffer, '\t');

						String[] line;

						int x = 0;
						int sampidIdx = -1;
						while((line = reader.readNext()) != null) { 

							if(!line[0].toUpperCase().contains("DBGAP")) continue;
							
							for(String col: line) {
								if(SAMPLE_HEADERS.contains(col.toUpperCase())) {
									sampidIdx = x;
									break;
								} else {
									x++;
								}
							}
							if(line[0].toUpperCase().contains("DBGAP")) break;
							
						}
						
						if(sampidIdx == -1) {
							System.err.println("No sample column for " + phsOnly);
							continue;
						}
						
						line = null;
						
						Set<String[]> sampleIdsList = new HashSet<>();
					

						while((line = reader.readNext()) != null) { 
							
							if(patientNumLookup.containsKey(line[0])) {
								
								if(line[sampidIdx].trim().isEmpty()) continue;
								// Only loading topmed freezes which are prefixed with NWD values
								// remove the continue clause below if other genomic data associated with dbgap data is ever consumed.
								if(!line[sampidIdx].startsWith("NWD")) continue;
								String[] arr = new String[] { patientNumLookup.get(line[0]), studyId, line[sampidIdx]};
								patientNums.add(patientNumLookup.get(line[0]));
								
								sampleIds.add(line[sampidIdx]);
								
								sampleIdsList.add(arr);
								
							}
							
						}
						if(!sampleIdsList.isEmpty()) {
							// add counts to collection to report number of sample ids found for the study
							sampleIdCountPerStudy.put(phsOnly, sampleIdsList.size());
							
							try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + phsOnly + "_SampleIds.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
							   for(String[] arr: sampleIdsList) {
								   writer.flush();
								   writer.write(toCsv(arr));
							   }
							}
							
						}

						Mapping mapping = new Mapping(phsOnly + "_SampleIds.csv:2","/_genomic_sample_id/" + phsOnly + "/", "", "TEXT", "");
						
						sampleIdMappings.add(mapping);
					}
				}
			}
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Sample_ID_Counts.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
			for(Entry<String,Integer> entry: sampleIdCountPerStudy.entrySet()) {
				writer.write(entry.getKey() + "=" + entry.getValue());
			}
			writer.flush();
			writer.close();
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "SampleId_mapping.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			
 			for(Mapping mout: sampleIdMappings) {
 				writer.write(mout.toCSV() + '\n');
 			}
 			writer.flush();
 			writer.close();
		}

		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_vcfIndex.tsv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			
			CSVWriter csv = new CSVWriter(writer, '\t');
			
			String[] headers = new String[] {"vcf_path","contig","isAnnotated","isGzipped","sample_ids","patient_ids","sample_relationship","related_sample_ids"};
			String[] line = new String[] {"","","","",String.join(",", sampleIds),String.join(",", patientNums),"",""};
			
			if(!sampleIds.isEmpty()) {
				csv.writeNext(headers);
				csv.writeNext(line);
			}
			
		}

	}

	private static Map<String, String> getPatientMapping(String studyId) throws IOException {
		Map<String,String> map = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyId.toUpperCase() + "_PatientMapping.v2.csv"))){
			
			//System.out.println(studyId.toUpperCase() + "_PatientMapping.v2.csv");
			CSVReader reader = new CSVReader(buffer);
			
			String[] arr;
			
			while((arr = reader.readNext()) != null) {
				if(arr.length > 2) {
					map.put(arr[0], arr[2]);
				}
			}
		}
		return map;
	}

	private static String getStudyAccessions(String phsOnly) throws IOException {
		
		List<BDCManagedInput> managedInputs = BDCJob.getManagedInputs();
		
		for(BDCManagedInput managedInput:managedInputs) {
			if(managedInput.getStudyIdentifier().equals(phsOnly)) {
				return managedInput.getStudyAbvName();
			}
		}
		
		return null;
	}

}
