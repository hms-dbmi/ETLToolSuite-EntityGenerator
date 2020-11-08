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
import etl.jobs.Job;
import etl.jobs.jobproperties.JobProperties;


/**
 * This class will generate all of the consent variables needed for 
 * BDC authorizations.
 * 
 * _consent - Accession numbers and consent groups for parent studies only.
 * _harmonized_consent - Accession Numbers and consent groups for studies included in the harmonized datasets
 * _topmed_consent - Accession numbers and consent groups for topmed studies only.
 * 
 * @author Tom DeSain
 *
 */
public class ConsentGroupGenerator extends BDCJob {
	/**
	 * 
	 */
	private static final long serialVersionUID = 364114251633959244L;
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
	
	private static List<String> HARMONIZE_OMISSION = new ArrayList<String>();
	static {
		HARMONIZE_OMISSION.add("MAYOVTE");
 	}	
	 
	private static Set<String> HARMONIZED_HPDS_IDS = new HashSet<String>();
	static {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "HRMN_allConcepts.csv"))) {
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				HARMONIZED_HPDS_IDS.add(line[0]);
			}
		} catch (IOException e) {
			System.err.println("Missing HRMN_allConcepts.csv file.");
			e.printStackTrace();
		}
		
	};
	
	static List<String[]> _harmonized_consents = new ArrayList<>();
	
	
	public static void main(String[] args) throws ParseException {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static void execute() throws IOException, ParseException {
		// read the spread sheet into a List<String[]>
		// Once GOOGLE API is ready to go replace this method with the google sheet methodology.
		List<BDCManagedInput> managedInputs = getManagedInputs();

		Map<String,Map<String,String>> patientMappings = getPatientMappings();
		
		//System.out.println("Building Parent Consents");
		
		Map<String,List<String[]>> consents = generateConsents(managedInputs, patientMappings);
		
		//List<String[]> _consents = 
		
		//System.out.println("Building Topmed Consents");
		//List<String[]> _topmed_consent = generateConsents(managedInputs, patientMappings,"topmed");
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(Entry<String, List<String[]>> entry: consents.entrySet()) {
				
				for(String[] line: entry.getValue()) {
					writer.write(toCsv(line));	
				}
				
			}
			
		}
		if(consents.containsKey("PARENT")) {
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "parent_consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(String[] line: consents.get("PARENT")) {
					writer.write(toCsv(line));
				}
			}
		}
		if(consents.containsKey("TOPMED")) {
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "topmed_consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(String[] line: consents.get("TOPMED")) {
					writer.write(toCsv(line));
				}
			}
		}
		if(_harmonized_consents != null) {
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "harmonized_consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(String[] line: _harmonized_consents) {
					writer.write(toCsv(line));
				}
			}
		}
		System.out.println("Finished building consents");
	}

	private static Map<String, List<String[]>> generateConsents(List<BDCManagedInput> managedInputs,
			Map<String,Map<String,String>> patientMappings) throws IOException, ParseException {
		
		Map<String,List<String[]>> consents = new HashMap<>();
		
		for(BDCManagedInput managedInput: managedInputs) {
			
			String studyType =  managedInput.getStudyType().toUpperCase();
			
			String studyAbvName = managedInput.getStudyAbvName();
			
			String studyIdentifier = managedInput.getStudyIdentifier();
			
			System.out.println("Building Consents for " + studyAbvName + " - " + studyIdentifier + " - " + studyType);
			
			if(consents.containsKey(studyType)) {
				consents.get(studyType).addAll(buildConsents(studyIdentifier,studyAbvName,patientMappings));
			} else {
				consents.put(studyType.toUpperCase(), new ArrayList<String[]>(buildConsents(studyIdentifier,studyAbvName,patientMappings)));
			}
			if(managedInput.getIsHarmonized().equalsIgnoreCase("Y") && !HARMONIZE_OMISSION.contains(studyAbvName.toUpperCase())) {
				
				addHarmonizedConsents(studyIdentifier,studyAbvName,patientMappings);
				
			}
		}
		return consents;
	}

	private static List<String[]> generateConsents(List<BDCManagedInput> managedInputs, Map<String,Map<String,String>> patientMappings, String consentType) throws IOException, ParseException {
		List<String[]> _consents = new ArrayList<>();

		for(BDCManagedInput managedInput: managedInputs) {
			System.out.println("Building Consents for " + managedInput.getStudyAbvName() + " - " + managedInput.getStudyIdentifier() + " - " + consentType);
			
			if(managedInput.getStudyType().equalsIgnoreCase(consentType)) {
				
				String studyAbvName = managedInput.getStudyAbvName();
				String studyIdentifier = managedInput.getStudyIdentifier();
				
				// hpds_id,consents value for parent
				//_consents.add(); consentLookup = buildParentConsents(studyAbvName, patientMappings);
				_consents.addAll(buildConsents(studyIdentifier,studyAbvName,patientMappings));
				
				if(managedInput.getIsHarmonized().equalsIgnoreCase("Y") && !HARMONIZE_OMISSION.contains(studyAbvName.toUpperCase())) {
				
					addHarmonizedConsents(studyIdentifier,studyAbvName,patientMappings);
					
				}
					
			}
			
		}
		return _consents;
	}

	
	private static void addHarmonizedConsents(String studyIdentifier, String studyAbvName,
			Map<String, Map<String, String>> patientMappings) throws IOException, ParseException {
		File dataDir = new File(DATA_DIR);
		
		List<String[]> returnSet = new ArrayList<>();
		
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
			
			if(fileNames.length == 1) {
				
			
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + fileNames[0]))) {
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
						
						if(CONSENT_HEADERS.contains(header.toLowerCase())) {
							consentidx = x;
							break;
						}
						x++;
						
					}
					
					if(consentidx != -1) {
					
						String[] line;
						while((line = reader.readNext()) != null) {
							
							if(line.length < consentidx) continue;
							if(line[consentidx].isEmpty()) continue;
							
							String hpds_id = Job.mappingLookup(line[0], patientMappings.get(studyAbvName));
							if(hpds_id == null || hpds_id.isEmpty()) {
								System.err.println(studyAbvName + " Patient is not mapped in current data load dbgap_id=" + line[0]);
							}
							if(!HARMONIZED_HPDS_IDS.contains(hpds_id)) continue; 
							
							String consentCode = "c" + line[consentidx];
							
							String consentVal = studyIdentifier + "." + consentCode;
							
							returnSet.add(new String[] { hpds_id, consentVal });
							
						}
						
					} else {
						throw new ParseException("Cannot find header for " + fileNames[0], new Throwable().fillInStackTrace());
					}
				}
			}
		} else {
			throw new IOException("parameter DATA_DIR = " + DATA_DIR + " is not a directory!", new Throwable().fillInStackTrace() );
		}
		_harmonized_consents.addAll(returnSet);	
	}

	private static List<String[]> buildConsents(String studyIdentifier, String studyAbvName, Map<String, Map<String, String>> patientMappings) throws IOException, ParseException {
		File dataDir = new File(DATA_DIR);
		
		List<String[]> returnSet = new ArrayList<>();
		
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
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + fileNames[0]))) {
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
					
					if(CONSENT_HEADERS.contains(header.toLowerCase())) {
						consentidx = x;
						break;
					}
					x++;
					
				}
				
				if(consentidx != -1) {
				
					String[] line;
					
					while((line = reader.readNext()) != null) {
						
						if(line.length < consentidx) continue;
						if(line[consentidx].isEmpty()) continue;
						
						String hpds_id = mappingLookup(line[0], patientMappings.get(studyAbvName));
						if(hpds_id == null) continue;
						String consentCode = "c" + line[consentidx];
						
						String consentVal = studyIdentifier + "." + consentCode;
						
						returnSet.add(new String[] { hpds_id, consentVal });
						
					}
					
				} else {
					throw new ParseException("Cannot find header for " + fileNames[0], new Throwable().fillInStackTrace());
				}
			}
			
		} else {
			throw new IOException("parameter DATA_DIR = " + DATA_DIR + " is not a directory!", new Throwable().fillInStackTrace() );
		}
		return returnSet;
	}



	protected static void setLocalVariables(String[] args, JobProperties properties) throws Exception {
		// look at super setVariable to see examples
	}
}
