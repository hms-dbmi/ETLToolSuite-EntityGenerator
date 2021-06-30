package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.PatientMapping;

public class HarmonizedPatientMappingGenerator extends BDCJob {
	
	private static Map<String,String> STUDY_ID_SYNONYM = new HashMap<>();
	
	private static Map<String,String> SUBJECT_ID_COL_OVERRIDE = new HashMap<>();
	
	static {
		STUDY_ID_SYNONYM.put("MAYOVTE", "Mayo_VTE");
		
		STUDY_ID_SYNONYM.put("AMISH", "Amish");
		
		STUDY_ID_SYNONYM.put("HCHSSOL", "HCHS_SOL");

		STUDY_ID_SYNONYM.put("COPDGENE", "COPDGene");

		
		SUBJECT_ID_COL_OVERRIDE.put("JHS", "SOURCE_SUBJECT_ID2");
	};
	
	
	public class HarmonizedPatientMapping {
		public String dbgapSubjectId = "";
		public String hpdsPatientNum = "";
		public String subjectId = "";
		
		public HarmonizedPatientMapping() {
			super();

		}
		
	}
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
		
		// hrmn patient mapping
		Set<PatientMapping> pms = new HashSet<PatientMapping>();

		Map<String,Map<String,String>> patientMappings = BDCJob.getPatientMappings();
		
		List<BDCManagedInput> managedInputs = BDCJob.getManagedInputs();
		
		Set<String> distinctSubjectIdsInHrmn = getDistinctSubjectIdsInHrmn();
		
		for(BDCManagedInput managedInput: managedInputs) {
			if(managedInput.equals("MAYOVTE")) continue;
			if(managedInput.getIsHarmonized().toUpperCase().startsWith("N")) continue;
			if(managedInput.getReadyToProcess().toUpperCase().startsWith("N")) continue;
			
			if(patientMappings.containsKey(managedInput.getStudyAbvName())) {
				
				String subjMultiFileName = BDCJob.getStudySubjectMultiFile(managedInput);
				
				Map<String,String> patientMapping = patientMappings.get(managedInput.getStudyAbvName());
				
				if(subjMultiFileName != null) {
					
					Map<String,String> dbgapSubjIdToSubjId = BDCJob.getDbgapToSubjectIdMappingFromRawData(managedInput,SUBJECT_ID_COL_OVERRIDE);
					
					for(Entry<String, String> entry : dbgapSubjIdToSubjId.entrySet()) {
						
						PatientMapping pm = new PatientMapping();
						
						if(patientMapping.containsKey(entry.getKey())) {
							String trialId = STUDY_ID_SYNONYM.containsKey(managedInput.getStudyAbvName()) ? 
									STUDY_ID_SYNONYM.get(managedInput.getStudyAbvName()): managedInput.getStudyAbvName();
							pm.setSourceId(trialId + "_" + entry.getValue());
							
							pm.setSourceName(managedInput.getStudyAbvName().toUpperCase());
							
							pm.setPatientNum(new Integer(patientMapping.get(entry.getKey())));
							
							if(distinctSubjectIdsInHrmn.contains(pm.getSourceId())) pms.add(pm);
							
							else System.out.println(pm.getSourceId() + " is not apart of harmonized data set");
						}
						
					}
					
					
				} else {
					
					System.out.println(managedInput.getStudyAbvName() + " patient subject multi missing.");

				}
				
			} else {
				
				System.out.println(managedInput.getStudyAbvName() + " patient mapping missing.");
				
			}
			
		}
		
		try(BufferedWriter bw = Files.newBufferedWriter(Paths.get(WRITE_DIR + "HRMN_PatientMapping.v2.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(PatientMapping pm : pms) {
				
				bw.write(pm.toCSV());
				
			}
			
		}
	}

	private static Set<String> getDistinctSubjectIdsInHrmn() throws IOException {
		Set<String> subjids = new HashSet<>();
		
		File dir = new File(DATA_DIR);
		
		File[] files = dir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if(name.startsWith("topmed_dcc_harmonized")) return true;
				return false;
			}
		});
		for(File f: files) {
			try(BufferedReader reader = Files.newBufferedReader(f.toPath())) {
				CSVReader csvreader = new CSVReader(reader);
				
				String[] line;
				while((line = csvreader.readNext()) != null ) {
					subjids.add(line[0]);
				}
			}
		}
		return subjids;
	}
}