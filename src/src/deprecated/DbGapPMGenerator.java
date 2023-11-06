package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.opencsv.CSVReader;

import etl.jobs.Job;


/**
 * old method used once
 * @author Tom
 *
 */
@Deprecated
public class DbGapPMGenerator extends Job {

	private static Map<String,Integer> SEQUENCE_MAP = new TreeMap<String,Integer>();
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			//setLocalVariables(args);
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
		
		File[] files = new File(DATA_DIR).listFiles(new FilenameFilter() {
			
			//apply a filter
			@Override
			public boolean accept(File dir, String name) {
				boolean result;
				if(name.toUpperCase().contains("SUBJECT.MULTI")){
					result=true;
				}
				else{
					result=false;
				}
				return result;
			}
		
		});
		for(File f: files) {
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(f.getAbsolutePath()))) {
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] line;
				
				while((line = reader.readNext()) !=null ) {
					
					sequencePatient(line[0]);
					
				}
			}
		}
		writePatientMapping();
		
	}
	
	private static void writePatientMapping() {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_PatientMapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			String[] stringToWrite = new String[3];
			
			for(Entry<String,Integer> entry: SEQUENCE_MAP.entrySet()) {
				
				stringToWrite[0] = entry.getKey();

				stringToWrite[1] = TRIAL_ID;
				
				stringToWrite[2] = entry.getValue().toString();
				
				writer.write(toCsv(stringToWrite));
			}
			
			writer.flush();
			
			writer.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static Integer sequencePatient(String patientNum) throws IOException {
				
		if(SEQUENCE_MAP.containsKey(patientNum)) {
			return SEQUENCE_MAP.get(patientNum);
		} else {
			SEQUENCE_MAP.put(patientNum, PATIENT_NUM_STARTING_SEQ);
			PATIENT_NUM_STARTING_SEQ++;
			return PATIENT_NUM_STARTING_SEQ - 1;
		}
	}
	

}
