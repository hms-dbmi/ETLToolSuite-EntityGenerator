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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.core.parser.ParseException;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.Mapping;

public class StudiesConsentsGenerator extends BDCJob {
	
	// generate a map that contains 
	// <Study Abv name + phsIdentifier, < consent name, Set of patient nums >>
	
	private static Map<String,Map<String,Set<String>>> patientSets = new HashMap<>();
	/**
	 * Contains all the variants of consent columns in lowercase format
	 */
	private static List<String> CONSENT_HEADERS = new ArrayList<String>();
	static {
		
		CONSENT_HEADERS.add("consent".toLowerCase());
		CONSENT_HEADERS.add("consent_1217".toLowerCase());
		CONSENT_HEADERS.add("consentcol");
		CONSENT_HEADERS.add("gencons");
		

	}
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}	
	
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException, ParseException {
		// iterate over managed inputs 
		List<BDCManagedInput> managedInputs = getManagedInputs();
		
		// gather patient mappings for all studies
		Map<String,Map<String,String>> patientMappings = getPatientMappings();
		
		System.out.println(patientMappings.size());
		System.out.println(patientMappings);
		Set<Mapping> mappings = new HashSet<Mapping>();
		
		Set<String> generatedMappings = new HashSet<>();
		
		mappings.add(new Mapping("rootNodeConsents.csv:1", "µ_studies_consentsµ", "", "TEXT", ""));
		
		generatedMappings.add("µ_studies_consentsµ");
		
		Set<String> rootNodePatients = new HashSet<>();
		
		for(BDCManagedInput input: managedInputs) {
			String firstLevelName = "µ_studies_consentsµ" + input.getStudyIdentifier() + "µ";

			mappings.add(new Mapping(input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_first_level.csv:1",
						firstLevelName,
						"",
						"TEXT",
						""
						)
					);

			generatedMappings.add(firstLevelName);
			
			Map<String,Set<String>> patSet = buildConsents(input.getStudyIdentifier(),input.getStudyAbvName(),patientMappings);
				
			Set<String> firstLevelPatients = new HashSet<>();
			
			for(Entry<String,Set<String>> entry: patSet.entrySet()) {
				
				if(entry.getKey().contains("Subjects did not participate in the study")) continue;
				
				String consentName = entry.getKey().replaceAll(".*\\(","").replaceAll("\\).*", "").trim();
				
				String consentLevelName = firstLevelName + consentName + 'µ';
				
				if(!generatedMappings.contains(consentLevelName)) {
					
					mappings.add(new Mapping(input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_" + consentName +".csv:1",
							consentLevelName,
							"",
							"TEXT",
							""
							)
						);
					generatedMappings.add(consentLevelName);

				}
				
				rootNodePatients.addAll(entry.getValue());
				firstLevelPatients.addAll(entry.getValue());
				
				try(BufferedWriter writer = Files.newBufferedWriter(
						Paths.get(WRITE_DIR + input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_" + consentName +".csv"), 
						StandardOpenOption.CREATE, 
						StandardOpenOption.TRUNCATE_EXISTING)) {
					
					for(String patNum: entry.getValue()) {
						
						writer.write(toCsv(new String[] {patNum, "TRUE"} ));
						
					}
					
				}
			}
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_first_level.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				
				for(String patNum: firstLevelPatients) {
					
					writer.write(toCsv(new String[] {patNum, "TRUE"} ));
					
				}
				
			}
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "rootNodeConsents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(String patNum: rootNodePatients) {
				
				writer.write(toCsv(new String[] {patNum, "TRUE"} ));
				
			}
			
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "studies_consents_mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(Mapping mapping: mappings) {
				
				writer.write(mapping.toCSV() + "\n");
				
			}
			
		}
	}
	
	private static Map<String,Set<String>> buildConsents(String studyIdentifier, String studyAbvName, Map<String, Map<String, String>> patientMappings) throws IOException, ParseException {
		
		File dataDir = new File(DATA_DIR + "decoded/" );
		
		Map<String,Set<String>> returnSet = new HashMap<>();
		
		if(dataDir.isDirectory()) {
			
			String[] fileNames = dataDir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith(studyIdentifier) && name.toLowerCase().contains("subject.multi") && name.toLowerCase().endsWith(".txt")) {
						return true;
					} else {
						return false;
					}
				}
			});
			
			if(fileNames.length != 1) {
				return returnSet;
				//sys("Expecting one subject.multi file per study aborting! : " + studyIdentifier, new Throwable().fillInStackTrace());
				
			}
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "decoded/" + fileNames[0]))) {
				CSVReader reader = new CSVReader(buffer);
				
				int consentidx = getConsentIdx(fileNames[0]);
				
				if(consentidx != -1) {
				
					String[] line;
					
					while((line = reader.readNext()) != null) {
						
						if(line.length < consentidx) continue;
						if(line[consentidx].isEmpty()) continue;
						
						String hpds_id = mappingLookup(line[0], patientMappings.get(studyAbvName));
						if(hpds_id == null) {
							System.err.println("No HPDS ID found for " + line[0]);
						};
						
						if(returnSet.containsKey(line[consentidx])) {
							returnSet.get(line[consentidx]).add(hpds_id);
						} else {
							Set<String> set = new HashSet<>();
							set.add(hpds_id);
							returnSet.put(line[consentidx], set);
						}						
					}
					
				} else {
					throw new ParseException("Cannot find header for " + fileNames[0], new Throwable().fillInStackTrace());
				}
			}
			
		} else {
			throw new IOException("parameter DATA_DIR = " + dataDir + " is not a directory!", new Throwable().fillInStackTrace() );
		}
		return returnSet;
	}

	private static int getConsentIdx(String fileName) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "raw/" + fileName))) {
			CSVReader reader = new CSVReader(buffer, '\t', 'π');
			
			String[] headers;
			
			while((headers = reader.readNext()) != null) {

				boolean isComment = ( headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() ) ? true: headers[0].isEmpty() ? true: false;
				
				if(isComment) {
					
					continue;
					
				} else {
					
					break;
					
				}
				
			}
			
			int consentidx = -1;
			int x = 0;
			
			for(String header: headers) {
				
				System.out.println("Checking for consents in Static Headers: " + header);

				if(CONSENT_HEADERS.contains(header.toLowerCase())) {
					consentidx = x;
					break;
				}
				x++;
			}
			x = 0;
			if(consentidx == -1) {
				
				System.out.println("Consent header not found in static block.  Searching Dynamically for consent header");
				for(String header: headers) {
					
					if(header.toLowerCase().contains("consent")) {
						System.out.println("Consent header found = " + header);
						consentidx = x;
						break;
					}
					x++;
				}				
				
			}
			
			return consentidx;
		}
	}
}
