package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.Job;

public class PHSIdGenerator extends BDCJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7961139229702008400L;

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
		// Once GOOGLE API is ready to go replace this method with the google sheet methodology.
		List<BDCManagedInput> managedInputs = getManagedInputs();

		Map<String,Map<String,String>> patientMappings = getPatientMappings();

		List<String[]> topmedAccessions = buildAccessions(managedInputs, patientMappings, "TOPMED");
		
		List<String[]> parentAccessions = buildAccessions(managedInputs, patientMappings, "PARENT");

		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Topmed_AccessionIds.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			for(String[] accessionid: topmedAccessions) {
				writer.write(toCsv(accessionid));
			}
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Parent_AccessionIds.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			for(String[] accessionid: parentAccessions) {
				writer.write(toCsv(accessionid));
			}
		}
		
		
	}
	


	private static List<String[]> buildAccessions(List<BDCManagedInput> managedInputs, Map<String, Map<String, String>> patientMappings, String accessionType) throws IOException {
		List<String[]> returnSet = new ArrayList<String[]>();
		
		for(BDCManagedInput managedInput: managedInputs) {
			
			if(!managedInput.getStudyType().equalsIgnoreCase(accessionType)) continue;
			
			String studyAbvName = managedInput.getStudyAbvName();
			
			if(!patientMappings.containsKey(managedInput.getStudyAbvName())) continue;
			
			String subjectMultiFile = getStudySubjectMultiFile(managedInput,DATA_DIR);
			
			if(subjectMultiFile == null) continue;
			// used phs subject id column to find index
			int subjectIdColIdx = BDCJob.findRawDataColumnIdx(Paths.get(DATA_DIR + subjectMultiFile),managedInput.getPhsSubjectIdColumn());
			
			if(subjectIdColIdx == -1) {
				System.err.println("Missing Subject id column for - " + managedInput.getStudyAbvName() + " - using column header " + managedInput.getPhsSubjectIdColumn());
			
				continue;
			}
			
			CSVReader reader = BDCJob.readRawBDCDataset(Paths.get(DATA_DIR + subjectMultiFile), true);
			
			String line[];
			
			while((line = reader.readNext()) != null) {
				if(line.length < subjectIdColIdx) continue;
				if(line[subjectIdColIdx].isEmpty()) continue;
			
				String hpds_id = mappingLookup(line[0], patientMappings.get(studyAbvName));
									
				String phsID = managedInput.getStudyIdentifier() + "." + subjectMultiFile.split("\\.")[1] + "_" + line[subjectIdColIdx]; 
				
				returnSet.add(new String[] { hpds_id, phsID });
				
			}
			
			
		}
		
		return returnSet;
	}

	

}
