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
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

public class SampleIdGenerator extends BDCJob {

	private static List<String> SAMPLE_HEADERS = new ArrayList<String>();
	static {
		
		SAMPLE_HEADERS.add("SAMPLE_ID");
		SAMPLE_HEADERS.add("SAMPLEID");
		SAMPLE_HEADERS.add("SAMPID");

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
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "SampleIds.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
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
	
							
							//////////
							String[] line;
	
							int x = 0;
							int sampidIdx = -1;
							while((line = reader.readNext()) != null) { 
	
								if(!line[0].toUpperCase().equals("DBGAP_SUBJECT_ID")) continue;
								
								for(String col: line) {
									if(SAMPLE_HEADERS.contains(col.toUpperCase())) {
										sampidIdx = x;
										break;
									} else {
										x++;
									}
								}
								if(line[0].toUpperCase().equals("DBGAP_SUBJECT_ID")) break;
								
							}
							
							if(sampidIdx == -1) {
								System.err.println("No sample column for " + phsOnly);
								continue;
							}
							
							line = null;
							
							while((line = reader.readNext()) != null) { 
								
								if(patientNumLookup.containsKey(line[0])) {
									
									if(line[sampidIdx].trim().isEmpty()) continue;
									if(!line[sampidIdx].startsWith("NWD")) continue;
									String[] arr = new String[] { patientNumLookup.get(line[0]), studyId, line[sampidIdx]};
									
									writer.write(toCsv(arr));
									
								}
								
							}
							writer.flush();
						}
					}
				}
			}
		}
	}

	private static Map<String, String> getPatientMapping(String studyId) throws IOException {
		Map<String,String> map = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyId.toUpperCase() + "_PatientMapping.v2.csv"))){
			CSVReader reader = new CSVReader(buffer);
			
			String[] arr;
			
			while((arr = reader.readNext()) != null) {
				
				map.put(arr[0], arr[2]);
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
