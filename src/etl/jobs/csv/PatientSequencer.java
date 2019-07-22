package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.job.entity.i2b2tm.PatientDimension;
import etl.job.entity.i2b2tm.PatientMapping;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * 
 * Generates sequences for the columns in the data file.
 *
 */
public class PatientSequencer extends Job {

	
	public static void main(String[] args) {
		try {
			setVariables(args,buildProperties(args));
		} catch (Exception e) {
			System.err.println(e);
		}
		
		try {
			if(DO_SEQUENCING) execute();
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	/**
	 *  PATIENT NUM SEQUENCED IN OBSERVATIONFACT.CSV AND PATIENTDIMENSION.CSV
	 *  WILL ALSO GENERATE THE PATIENT_MAPPING IS PATIENT NUM IS SEQUENCED.  
	 *  IF THIS IS NOT SEQUENCE PATIENT_MAPPING WILL NOT BE GENERATED.
	 */
	private static void execute() {

		if(DO_PATIENT_NUM_SEQUENCE) {
			try {
				sequencePatients();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void sequencePatients() throws Exception {
		// LOAD ALL POSSIBLE PATIENT NUMS INTO A MAP<STRING,STRING> 
		Map<String,String> patientMap = new HashMap<String,String>();
		List<PatientDimension> patients = new ArrayList<PatientDimension>();
		List<PatientMapping> patientMapppings = new ArrayList<PatientMapping>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "PatientDimension.csv"))){
			
			CsvToBean<PatientDimension> csvToBean = 
					Utils.readCsvToBean(PatientDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
			patients = csvToBean.parse();
									
			patients.forEach(pd ->{
				
				patientMap.put(pd.getPatientNum(), String.valueOf(PATIENT_NUM_STARTING_SEQ));
				
				String sourcePatNum = pd.getPatientNum();
				String mappedPatNum = String.valueOf(PATIENT_NUM_STARTING_SEQ);
				
				pd.setPatientNum(mappedPatNum);
				pd.setSourceSystemCD(TRIAL_ID + ":" + PATIENT_NUM_STARTING_SEQ);
				// Create mapping
				PatientMapping pm = new PatientMapping();
				
				pm.setPatientIde(sourcePatNum);
				pm.setPatientIdeSource(TRIAL_ID);
				pm.setPatientNum(mappedPatNum);
				patientMapppings.add(pm);
				
				PATIENT_NUM_STARTING_SEQ++;
			});
		}

		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "PatientDimension.csv"))){
			
			Utils.writeToCsv(buffer, patients, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 

		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "PatientMapping.csv"))){
			
			Utils.writeToCsv(buffer, patientMapppings, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
	}
}
