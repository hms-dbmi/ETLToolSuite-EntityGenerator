package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections4.map.HashedMap;

import com.opencsv.CSVReader;

public class ConsentGroupGenerator extends Job {
	private static Map<String,String> PHS_TO_STUDY_ID = new HashedMap<String,String>();
	
	private static List<String> CONSENT_HEADERS = new ArrayList<String>();
	
	private static String STUDY_ID_W_ACCESSIONS = "studyid_with_accessions.csv";

	private static String CURRENT_STUDY = "";
	
	static {
		
		CONSENT_HEADERS.add("consent".toLowerCase());
		CONSENT_HEADERS.add("consent_1217".toLowerCase());
		CONSENT_HEADERS.add("consentcol");
		CONSENT_HEADERS.add("gencons");
		

	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5109126327540663739L;


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
		// generate consent groups
		Set<String[]> consentGroups = buildConsentGroups();
		
		//System.out.println(consentGroups.size());
		Map<String,String> dbgapIdToI2B2Id = buildMappingLookup();
		
		consentGroups = syncIds(consentGroups,dbgapIdToI2B2Id);

		writeVariable(consentGroups);
		
	}


	/**
	 * This method will generate the consent group data that is needed to generate
	 * the data file used to make the consent group concept in the mapping file.
	 * 
	 * @return
	 * @throws IOException
	 */
	private static Set<String[]> buildConsentGroups() throws IOException {
		
		Set<String[]> consentVar = new HashSet<String[]>();
		
		File fil = new File(DATA_DIR);
		
		if(fil.isDirectory()) {
			
			for(File f: new File(DATA_DIR).listFiles()) {
				
				if(f.getName().toLowerCase().contains("subject.multi") && f.getName().toLowerCase().contains(".txt")) {
					
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + f.getName()))){
						
						CSVReader reader = new CSVReader(buffer, '\t', 'π');
						
						String[] headers;
						
						while((headers = reader.readNext()) != null) {
	
							boolean isComment = headers[0].toString().startsWith("#") ? true: headers[0].isEmpty() ? true: false;
							
							if(isComment) {
								
								continue;
								
							} else {
								
								break;
								
							}
							
						}
						
						int consentidx = -1;
						int x = 0;
						
						for(String header: headers) {
							
							if(CONSENT_HEADERS.contains(header.toLowerCase())) {
								consentidx = x;
								break;
							}
							x++;
							
						}
						if(consentidx != -1) {
							
							String[] fnamearr = f.getName().split("\\.");
							
							String[] line;
							
							String phsbeforeconsent = fnamearr[0] + "." + fnamearr[1] + "." + fnamearr[4] + ".c";
							
							String phsbeforeconsent2 = fnamearr[0] + ".c";
							
							while((line = reader.readNext()) != null) {
								
								if(line[consentidx].isEmpty()) {
									System.err.println("dbgap id " + line [0] + " does not have a consent for " + phsbeforeconsent);
								}
								
								String[] record = new String[2];
								
								
								//record[0] = line[0];
								
								//record[1] = phsbeforeconsent + line[consentidx];
								
								//record[2] = TRIAL_ID;
								
								//consentVar.add(record);
								
								//record = new String[3];
								
								
								record[0] = line[0];
								
								record[1] = phsbeforeconsent2 + line[consentidx];
																
								consentVar.add(record);
							}
							
						} else {
							System.err.println("No consent column found for: " + f.getName());
						}
						
					}
				}
			}
			return consentVar;
		} else {
			
				try {
					throw new Exception(DATA_DIR + " IS NOT A DIRECTORY!  CHECK CONFIG!");
				} catch (Exception e) {
					
					e.printStackTrace();
					
					System.err.println(e);
					
				}
				
			
		}
		return consentVar;
	}

	private static Map<String, String> buildMappingLookup() throws IOException {
		
		File file = new File(WRITE_DIR + TRIAL_ID + "_PatientMapping.csv");
		
		System.out.println();
		
		Map<String,String> mlookup = new HashMap<String,String>();
		
		if(file.exists()) {
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))){
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] line;
				
				while((line = reader.readNext()) != null){
					
					mlookup.put(line[0], line[2]);
					
				}
			}
	
		}
		return mlookup;
	}

	private static Set<String[]> syncIds(Set<String[]> consentGroups, Map<String, String> dbgapIdToI2B2Id) {
		
		Set<String[]> synced = new HashSet<String[]>();
		for(String[] str: consentGroups) {
		
			String dbgapid = str[0];
			
			if(dbgapIdToI2B2Id.containsKey(dbgapid)) {
				
				str[0] = dbgapIdToI2B2Id.get(dbgapid);
				
				synced.add(str);
				
			} else {
				
				System.err.println(dbgapid + " does not exist in PatientMapping.csv");
				synced.add(str);
			}
			
		}
		return synced;
	}

	private static void writeVariable(Set<String[]> consentGroups) throws IOException {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "ConsentGroupVariable.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			String[] headers = new String[2];
			
			for(String[] arr: consentGroups) {
				
				writer.write(toCsv(arr));
				
			}
		}
		
	}

	private static Map<String,String> buildAccessionsLookup() throws IOException{
		Map<String,String> accessions = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + STUDY_ID_W_ACCESSIONS))){
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] arr = line.split(",");
				
				if(arr.length == 2) {
					accessions.put(arr[0], arr[1]);
				}
				
			}
		}
		return accessions;
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
}
