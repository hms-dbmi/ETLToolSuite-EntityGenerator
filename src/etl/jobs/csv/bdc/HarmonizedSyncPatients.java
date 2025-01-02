package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.job.entity.hpds.AllConcepts;
import etl.jobs.Job;

public class HarmonizedSyncPatients extends Job {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		
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
		
		// read hrmn patient mapping
		List<String[]> hrmnPatientMapping = getPatientMappings();
		// create a lookup map of HPDS_PATIENT_NUM, SOURCE_ID
		// The source id will be converted to the id of individual <study_id>_PatientMapping file.
		
		// read the HarmonizedPatientsWithConsentInfo.csv file
		List<String[]> hrmnConsentInfo = getConsentInfo();
		//validateConsentInfo(hrmnConsentInfo);
		Map<String, String> patIdMap = syncPatientMapping(hrmnPatientMapping, hrmnConsentInfo);
		// Update HRMN_allConcepts with the HRMNPatientNumMapping.csv
		System.out.println("updating HRMN_allConcepts");
		updateHRMNAllConcepts(patIdMap);
		System.out.println("Finished Updating.");
	}
	
	private static void updateHRMNAllConcepts(Map<String, String> patIdMap) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "HRMN_allConcepts.csv"))){
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader reader = builder.build();
						
			String[] line;

			Set<AllConcepts> records = new TreeSet<>( new Comparator<AllConcepts>() {

				@Override
				public int compare(AllConcepts o1, AllConcepts o2) {
					if(o1 == null || o2 == null) return -1;
					
					int conceptpath = o1.getConceptPath().compareTo(o2.getConceptPath());
					
					if(conceptpath != 0) {
						return conceptpath;
					}
					
					return o1.getPatientNum().compareTo(o2.getPatientNum());
											
				}
				
			} ); 
			while((line = reader.readNext()) != null) {
				
				AllConcepts ac = new AllConcepts();

				ac.setPatientNum(new Integer(line[0]));
				ac.setConceptPath(line[1]);
				ac.setNvalNum(line[2]);
				ac.setTvalChar(line[3]);
				
				String hrmnId = line[0];
				
				if(patIdMap.containsKey(hrmnId)) {
					ac.setPatientNum(new Integer(patIdMap.get(hrmnId)));
					records.add(ac);
				} else {
					//records.add(ac);
					System.err.println("No matching id for " + line[0]);
				}
			}
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "HRMN_allConcepts.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
				
				for(AllConcepts ac: records) {
					
					writer.write(ac.toCSV());
					
				}
				
			}
			
		}
		
	}

	private static Map<String,String> syncPatientMapping(List<String[]> hrmnPatientMapping, List<String[]> hrmnConsentInfo) throws IOException {
		// study ids
		Set<String> studyids = new HashSet<String>();
		
		for(String[] line: hrmnConsentInfo) {
			if(line.length < 3) continue;
			
			studyids.add(line[3].toUpperCase());
			
		}
		
		Map<String,String> hrmnId_dbgapId = new HashMap<String,String>();

		// source id, dbgap id
		Set<String> distinctPatients = new HashSet<String>();
		for(String[] line: hrmnConsentInfo) {
			
			//if(line.length < 6) continue;
			if(line[2].isEmpty()) continue;
			if(line[2] == null) continue;

			hrmnId_dbgapId.put(line[0], line[2]);
			distinctPatients.add(line[0]);
		}
		System.out.println("Distinct Patients in Harmonized Consent file: " + distinctPatients.size());
		
		distinctPatients = new HashSet<String>();

		for(String[] record: hrmnPatientMapping) {
			
			distinctPatients.add(record[0]);

		}
		
		System.out.println("Distinct Patients in Harmonized Patient Mapping: " + distinctPatients.size());

		// hrmn hrmn_subj_id, hrmn_hpds_id, dbgap_id 
		// will convert to study's hpds_id joining these by dbgap_id 
		//Map<String,String> hrmnids = new HashMap<String,String>();
		
		Map<String,String[]> hrmnidsmap = new HashMap<String,String[]>();
		
		Set<String[]> hrmnids = new HashSet<String[]>();
		
		int missingids = 0;
		
		for(String[] record: hrmnPatientMapping) {
			if(hrmnId_dbgapId.containsKey(record[0])) {
			
				String[] hrmnid = new String[4];
				//hrmn_subj_id
				hrmnid[0] = record[0];//;record[0].split("\\_")[0] + "_" + hrmnId_dbgapId.get(record[0]);
				// hrmn_hpds_id
				hrmnid[1] = record[2];

				// the dbgapid for patient
				hrmnid[2] = hrmnId_dbgapId.get(record[0]);
					
			
				String[] t =  {hrmnid[0], hrmnid[1] };
				hrmnidsmap.put(hrmnid[2], t);
				hrmnids.add(hrmnid);
			
			} else {
				System.err.println(record[0] + " does not exist in the Harmonized Consent File.");
				missingids++;
			}
			
		}		
		System.err.println("missingids found: " + missingids);
		System.out.println(hrmnids.size());
		System.out.println(hrmnidsmap.size());
		// hrmn hpds_id, individual study id
		Map<String,String> patidMapping = new HashMap<String,String>();

		for(String studyid: studyids) {
			if(!Files.exists(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))) {
				System.err.println(studyid.toUpperCase() + " study is missing.  Make sure the shortname for trial id is correct in the consent info file.");
				continue;
			}
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyid.toUpperCase() +"_PatientMapping.v2.csv"))){
				
				// study's_hpds_id, dbgap_id
						
				try (CSVReader reader = new CSVReader(buffer, ',', '\"', '√')) {
					String[] line;
					while((line = reader.readNext()) != null) {
											
						if(line.length < 3) continue;
						
						String dbgapid = line[0];
						
						if(hrmnidsmap.containsKey(dbgapid)){
							
							String[] arr = hrmnidsmap.get(dbgapid);
							
							patidMapping.put(arr[1],line[2]);
							
						}
						/*
						for(String[] hrmnid: hrmnids) {
							
							if(dbgapid.equals(hrmnid[2])) {
								
								patidMapping.put(hrmnid[1], line[2]);
								break;
							}
							
						}
						/*
						if(hrmnids.containsKey(studyid.toUpperCase() + "_" + line[0])) {

							patidMapping.put(hrmnids.get(studyid.toUpperCase() + "_" + line[0]), line[2]);
							
						} else {
							//System.out.println(studyid.toUpperCase() + "_" + line[0] + " not found.");
						}
						*/
					}
				}
			}
	
		}
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "HRMNPatientNumMapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			String[] header = new String[2];
			
			header[0] = "hrmn_hpds_id";
			header[1] = "study_hpds_id";
			
			for(Entry<String,String> entry: patidMapping.entrySet()) {
				
				String[] towrite = new String[2];
				
				towrite[0] = entry.getKey();
				
				towrite[1] = entry.getValue();
				
				buffer.write(toCsv(towrite));
			}
			
			buffer.flush();
			
		}
		return patidMapping;
	}

	private static List<String[]> getConsentInfo() throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "HarmonizedPatientsWithConsentInfo.csv"))) {
			
			try (CSVReader reader = new CSVReader(buffer, ',', '\"', '√')) {
				return reader.readAll();
			}
			
		}
		
	}

	private static List<String[]> getPatientMappings() throws IOException {
	
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "HRMN_PatientMapping.v2.csv"))) {
		
			try (CSVReader reader = new CSVReader(buffer, ',', '\"', '√')) {
				return reader.readAll();
			}
			
		}
	}


	protected static char[] toCsv(String[] line) {
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
		
		return sb.toString().toCharArray();
	}
}
