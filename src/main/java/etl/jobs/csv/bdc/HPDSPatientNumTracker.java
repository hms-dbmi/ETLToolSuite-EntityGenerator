package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.jobproperties.JobProperties;
/**
 * This class will handle keeping tabs of patient nums used. 
 * 
 * This will track patient nums used.
 * 
 * BDC projects will assign a hpds patient num for each patient with a dbgap subject id from the multi files
 * 
 * 
 * @author Tom
 *
 */
public class HPDSPatientNumTracker extends BDCJob {


	
	private static  String PATIENT_NUM_FILE = "PatientPool.txt";

	private static Set<Integer> PATIENT_NUMS = new TreeSet<>();
	
	private static Boolean IS_REVERSE_ENGINEER = false;
	
	private static Integer CURRENT_PATIENT_NUM = 1;
	
	public static void main(String[] args) {
		try {
			
			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}
		
		try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			e.printStackTrace();
		} 
	}


	private static void execute() throws IOException {
		if(IS_REVERSE_ENGINEER) {
			reverseEngineerFromPatientMappings();
		} else {
			
			// build patient mapping and add to the patient num file
			populatePatientNums();
			
			List<BDCManagedInput> managedInputs = getManagedInputs();
			
			for(BDCManagedInput managedInput: managedInputs) {
				
				//if(NON_DBGAP_STUDY.contains(managedInput.getStudyAbvName().toUpperCase())) continue;
				
				if(managedInput.getDataProcessed().toUpperCase().startsWith("Y")) continue;
				if(managedInput.getReadyToProcess().toUpperCase().startsWith("N")) continue;
				
				List<String[]> pms = updatePatientMapping(managedInput);
				
				try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + managedInput.getStudyAbvName().toUpperCase() + "_PatientMapping.v2.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				
					for(String[] pm: pms) {
						if(pm[0].trim().isEmpty()) continue;
						writer.write(toCsv(pm));
					
					}
				
				}

			}
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + PATIENT_NUM_FILE), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(Integer i: PATIENT_NUMS) {
					writer.write(i.toString()+'\n');
				}
			}
			
		}
	}

	private static List<String[]> updatePatientMapping(BDCManagedInput managedInput) throws IOException {
		
		List<String[]> pms = new ArrayList<String[]>();

		if(Files.exists(Paths.get(DATA_DIR + managedInput.getStudyAbvName().toUpperCase() + "_PatientMapping.v2.csv"))) {
			
			pms = BDCJob.getPatientMappings(managedInput.getStudyAbvName().toUpperCase());
			
		}
		
		Set<String> currentIds = new TreeSet<String>();
		
		for(String[] pm: pms) {
			currentIds.add(pm[0]);
		}
		
		Set<String> patientIdsFromSource = getPatientSet(managedInput);
		
		int patientsCreated = 0;
		int patientsExisting = 0;
		
		for(String patId: patientIdsFromSource) {
			if(!currentIds.contains(patId)) {
				// new patient add to patient nums master file and create a patient mapping
				while(PATIENT_NUMS.contains(CURRENT_PATIENT_NUM)) {
					CURRENT_PATIENT_NUM++;
				}
				
				String[] newPm = new String[]{patId, managedInput.getStudyAbvName().toUpperCase(), CURRENT_PATIENT_NUM.toString()};
				
				pms.add(newPm);
				patientsCreated++;
				PATIENT_NUMS.add(CURRENT_PATIENT_NUM);
				
			}
			else {
				//keep record of existing participants for error handling
				patientsExisting++;
			}
			
		}
		System.out.println(patientsExisting + " existing subjects identified for: " + managedInput);
		System.out.println("Created " + patientsCreated + " subjects for: " + managedInput);
		if(patientsCreated == 0 & patientsExisting == 0){
			System.err.println("Patients unable to be sequenced. Please verify the format of the subject.multi and/or data files.");
			System.exit(-1);
		}
		return pms;
	}
	private static Set<String> getPatientSet(BDCManagedInput managedInput) throws IOException {
		// ADDING code to handle nondbgap studies
		//if(NON_DBGAP_STUDY.contains(managedInput.getStudyAbvName().toUpperCase())) return new HashSet<>();
		if(!managedInput.hasSubjectMultiFile()) {
			// READ ALL DATA SETS IN DATA DIR AND COLLECT SET OF SUBJECT IDS
			return getGenericPatientSet(managedInput);
		}
		
		// if missing subject multi file fail job completely as it is critical to have all patient counts for new data load
		if(BDCJob.getStudySubjectMultiFile(managedInput) == null) {
			throw new IOException("Critical error: " + managedInput.toString() + 
					" missing subject multi file at " + DATA_DIR + "decoded/" + "! All compliant/semi-compliant studies must have a subject multi file to proceed ahead!");
		}
		System.out.println("Getting patients from subject multi file");
		Set<String> patientSet = new HashSet<>();
		
		try(CSVReader reader = readRawBDCDataset(Paths.get(DATA_DIR + "decoded/" + BDCJob.getStudySubjectMultiFile(managedInput)),true)){
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				System.out.println("Subject multi file line0(DBGap Subject Id): " + line[0]);
					if(NumberUtils.isCreatable(line[0])) patientSet.add(line[0]);								
			}
			
		}
		return patientSet;
	}

	private static Set<String> getGenericPatientSet(BDCManagedInput managedInput) throws IOException {
		File f = new File(DATA_DIR);
		
		Set<String>  patientSet = new HashSet<>();
		System.out.println("Getting generic patient set");
		if(f.isDirectory()) {
			File[] files = new File(DATA_DIR).listFiles(new FilenameFilter() {
				
				//apply a filter
				@Override
				public boolean accept(File dir, String name) {
					boolean result;
					if(name.toUpperCase().endsWith(".CSV")){
						if(name.contains("Managed_Inputs.csv")) {
							return false;
						}
						result=true;
					}
					else{
						result=false;
					}
					return result;
				}
			
			});
			for(File file: files) {
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
					try (CSVReader reader = new CSVReader(buffer)) {
						String[] line;
						line = reader.readNext();
						if(line == null) continue;
						while((line = reader.readNext()) != null) {
							if(line.length -1 >= PATIENT_COL) {
								if(!line[0].trim().isEmpty() ) patientSet.add(line[PATIENT_COL]);
							}
						}
					}
					
				}
				
			}
		}
		return patientSet;
	}


	private static void populatePatientNums() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + PATIENT_NUM_FILE))) {
			String line;
			
			while((line = buffer.readLine())!=null) {
				if(NumberUtils.isCreatable(line)) {
					PATIENT_NUMS.add(Integer.parseInt(line));
				} else {
					System.err.println("Invalid patient num: " + line);
				}
			}
		}
	}


	private static void reverseEngineerFromPatientMappings() throws IOException {
		
		File f = new File(DATA_DIR);
		
		if(f.isDirectory()) {
			File[] files = new File(DATA_DIR).listFiles(new FilenameFilter() {
				
				//apply a filter
				@Override
				public boolean accept(File dir, String name) {
					boolean result;
					if(name.toUpperCase().endsWith("PATIENTMAPPING.V2.CSV")){
						result=true;
					}
					else{
						result=false;
					}
					return result;
				}
			
			});
			
			for(File file: files) {
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
					try (CSVReader reader = new CSVReader(buffer)) {
						String[] line;
						
						while((line = reader.readNext()) != null) {
							if(line.length < 3) continue;
							if(!NumberUtils.isCreatable(line[2])) continue;
							if(!PATIENT_NUMS.add(Integer.parseInt(line[2]))) {
								System.err.println("Duplicate patient num in data set found " + line[0] + "," + line[1] + "," + line[2]);
								throw new IOException("INVALID PATIENT MAPPINGS.  FIX DUPLICATE PATIENT NUMS");
							}
						}
					} catch (NumberFormatException e) {
						
						e.printStackTrace();
					}
					
				}
				
			}
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "PatientPool.txt"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(Integer patnum: PATIENT_NUMS) {
					System.out.println(patnum.toString());
					writer.write(patnum.toString() + '\n');
				}
			}
		} else {
			System.err.println(DATA_DIR + "is an invalid directory.");
		}
		
	}


	private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
		for(String arg: args) {
			if(arg.equalsIgnoreCase( "--revengineer" )){
				IS_REVERSE_ENGINEER = true;
			}
			
		}
	}
}
