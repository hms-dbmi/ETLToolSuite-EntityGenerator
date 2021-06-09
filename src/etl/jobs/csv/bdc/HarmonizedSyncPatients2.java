package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

public class HarmonizedSyncPatients2 extends BDCJob {

	public static Map<String,String> STUDY_ID_SYNONYM = new HashMap<>();
	{
		STUDY_ID_SYNONYM.put("MAYO", "MAYOVTE");
	};
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			setClassVariables(args);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		try {
			execute();
		} catch (IOException e) {
			
			System.err.println(e);
			
		}	
	}

	private static void execute() throws IOException {
		
		// headers for eav
		// SUBJECT_ID	unique_subject_key	topmed_study	dcc_harmonization_id	source_study_accession	source_study_version	variable	value
		List<String[]> harmonizedDemographicEavData = harmonizedDemographicEavData();
		
		Map<String,String> hrmnIdToDbGapId = buildHrmnIdToDbGapId(harmonizedDemographicEavData);
		

		
		//updateHrmnConcepts(hrmnPatientMapping);
	}

	/***
	 * 
	 * read the entire topmed_dcc_harmonized_demographic_v3_eav.txt file
	 * 
	 * use the phs number associated with subject id to get dbgap id
	 * 
	 * use this dbgap id to build a lookup map by joining it with patient mapping file.
	 *
	 * 
	 * @return
	 * @throws IOException
	 */
	
	private static List<String[]> harmonizedDemographicEavData() throws IOException {
		
		Map<String,String> lookup = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "topmed_dcc_harmonized_demographic_v3_eav.txt"))) {
		
			CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
	
			reader.readNext();
	
			return reader.readAll();			
		}		
		
		
	}

	private static Map<String, String> buildHrmnIdToDbGapId(List<String[]> harmonizedDemographicEavData) {
		
		// build a map with from all multi like so
		/*
		 * Map<String,Map<String,String>>
		 * 
		 * Map<phsstudyidentifer,Map<unique_subject_id,dbgap_subject_id>
		 * 
		 * iterate over each subject multi file in harmonized and add the unique_subject_id and dbgap subject id to the inner map
		 *   - use the subject id column in harmonizedDemographicEavData to match in each multi file
		 * 
		 * 
		 * add the dbgap subject id to each data file in harmonized
		 * 
		 * 
		 * 
		 * examplee
		 * phs000200: WHI_750217 
		 */
		// Map<String,Map<String,String>> 
		
		for(String[] hded: harmonizedDemographicEavData) {
			
		}
		
		return null;
	}

	/*
	private static void updateHrmnConcepts(Map<String, String> hrmnPatientMapping) throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "HRMN_allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + "HRMN_allConcepts.csv"))) {
			
				CSVReader csvreader = new CSVReader(reader);
				
				String[] line;
				
				while((line = csvreader.readNext()) != null ) {
					
					if(hrmnPatientMapping.containsKey(line[0])) {
					
						line[0] = hrmnPatientMapping.get(line[0]);
						
						writer.write(toCsv(line));
					
					} else {
						System.err.println("patient missing from mapping " + line[0]);
					}
				
				}
			
			}
			
		}
		
	}
	
	// returns the hrmn hpds id to the study hpds id
	private static Map<String,String> getHRMNPatientMappings() throws IOException {
		Map<String,String> rs = new HashMap<>();
		List<String[]> hrmnPatNums = new ArrayList<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "HRMN_PatientMapping.v2.csv"))) {
		
			CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
			
			hrmnPatNums = reader.readAll();
			
		}
		
		List<String[]> subjectIdLookup = getSubjectIdLookup();
		
		Set<String> missingStudies = new HashSet<String>();
		
		for(String[] patNums: hrmnPatNums) {
			// get normal subject id
			if(subjectIdLookup.containsKey(patNums[0])) {
				
				String subjectId = subjectIdLookup.get(patNums[0]);
				
				String trialId = patNums[0].split("\\_")[0];
				
				Object[] subjMulti = getStudyFile(trialId);
				
				if(subjMulti==null) {
					missingStudies.add(trialId);
				} else {
					
					String studyHpdsId = findStudyHpdsId(subjMulti, subjectId);
					
					rs.put(patNums[0], studyHpdsId);
				}
				
			} else {
				System.err.println("Missing mapping for " + patNums[0]);
			}
			
		}
		for(String ms: missingStudies) {
			System.err.println("Missing study or incomplete data set: " + ms);
		}
		return rs;
		
		
		
	}
	
	private static String findStudyHpdsId(Object[] subjMulti, String subjectId) throws IOException {
		File file = (File) subjMulti[0];
		BDCManagedInput mi = (BDCManagedInput) subjMulti[1];
		CSVReader reader = BDCJob.readRawBDCDataset(file, true);
		
		int idx = BDCJob.findRawDataColumnIdx(file.toPath(), mi.getPhsSubjectIdColumn());
		
		String[] line;
		String dbgapId = "";
		while((line = reader.readNext()) != null) {
			if(line.length >= idx) {
				
				String currSubj = line[idx];
				
				if(currSubj.equals(subjectId)) {
					dbgapId = line[0];
					break;
				}
			
			}
		}
		
		if(!dbgapId.isEmpty()) {
			List<String[]> pms = getPatientMappings(mi.getStudyAbvName());
			if(!pms.isEmpty()) {
				for(String[] pm: pms) {
					
					if(pm[0].equalsIgnoreCase(dbgapId)) return pm[2];
				}
			} else {
				System.err.println("Patient Mapping is empty " + mi.getStudyAbvName());
			}
		}
		
		System.err.println("missing subject in raw data set for " + subjectId);
		return null;
	}
	
	private static Object[] getStudyFile(String trialId) throws IOException {
	
		List<ManagedInput> managedInputs = ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);
		
		for(ManagedInput managedInput: managedInputs) {
			BDCManagedInput mi = (BDCManagedInput) managedInput;
			if(STUDY_ID_SYNONYM.containsKey(trialId.toUpperCase())) {
				trialId = STUDY_ID_SYNONYM.get(trialId);
			}
			if(mi.getStudyAbvName().equalsIgnoreCase(trialId)) {
				if(mi.getIsHarmonized().equalsIgnoreCase("y") && mi.getReadyToProcess().equalsIgnoreCase("yes")) {
					
					File dataDir = new File(DATA_DIR);
					File[] subjfile = dataDir.listFiles(new FilenameFilter() {
						
						@Override
						public boolean accept(File dir, String name) {
							if(name.contains(mi.getStudyIdentifier())) {
								if(name.toLowerCase().contains("multi")) {
									if(name.toLowerCase().contains("subject")) {
										if(name.endsWith("txt")) {
											return true;
										}
										
									}
								}
							}
							return false;
						}
					});
					
					if(subjfile.length >= 1) {
						return new Object[] { subjfile[0] ,  mi };
					} else {
						System.out.println("No subject multi file found for " + trialId);
					}
					
				} else {
					continue;
				}
			}
		}
		
		return null;
	}
	
	
	*/
	
	private static void setClassVariables(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
