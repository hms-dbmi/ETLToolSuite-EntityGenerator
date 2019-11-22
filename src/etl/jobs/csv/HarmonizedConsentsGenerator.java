package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

public class HarmonizedConsentsGenerator extends Job{

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
		Map<String,String> accensions = readAccensions();
		
		List<String[]> hrmnPatients = readHRMNPatientMapping();
		
		List<String[]> consentlist = buildHarmonizedConsents(accensions, hrmnPatients);
		
		writeVariable(consentlist);
	}

	private static List<String[]> buildHarmonizedConsents(Map<String, String> accensions, List<String[]> hrmnPatients) throws IOException {
		List<String[]> list = new ArrayList<String[]>();
		Map<String,String> consentLookup = new HashMap<String,String>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "harmonizedconsents.csv"))){
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				
				if(line.length > 2) consentLookup.put(line[0], line[2]);
				
			}
			
			
		}
		
		for(String[] patient: hrmnPatients) {
			
			if(consentLookup.containsKey(patient[0])) {
				if(accensions.containsKey(patient[0].split("_")[0].toUpperCase())) {
					String phs = accensions.get(patient[0].split("_")[0].toUpperCase());
					String consent = phs + consentLookup.get(patient[0]);
					
					String[] record = new String[3];
					record[0] = patient[2];
					record[1] = consent;
					record[2] = "HRMN";
					list.add(record);
					//consentname = 
				}
			}
			
		}
		
		return list;
	}

	private static List<String[]> readHRMNPatientMapping() throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "HRMN_PatientMapping.csv"))){
			
			CSVReader reader = new CSVReader(buffer);
			
			return reader.readAll();
			
		}
		
	}

	private static Map<String, String> readAccensions() throws IOException {
		
		Map<String,String> map = new HashMap<String,String>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "accensions.csv"))){
		
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				
				if(line.length > 1) map.put(line[1].trim(), line[0].trim());
				
			}
			
		}
		
		return map;
	}
	
	private static String toCsv(String[] line) {
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
		
		return sb.toString();
	}
	
	private static void writeVariable(List<String[]> consentGroups) throws IOException {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "ConsentGroupVariable.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			String[] headers = new String[3];
			
			//headers[0] = "patient_num";
			//headers[1] = "consent_var";
			//headers[2] = "trial_id";
			
			//writer.write(toCsv(headers));
			
			for(String[] arr: consentGroups) {
				
				writer.write(toCsv(arr));
				
			}
		}
		
	}
}
