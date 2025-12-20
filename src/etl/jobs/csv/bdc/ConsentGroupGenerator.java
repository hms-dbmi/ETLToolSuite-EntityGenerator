package etl.jobs.csv.bdc;

import com.opencsv.CSVReader;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.jobproperties.JobProperties;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;


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

	// Study type constants
	private static final String STUDY_TYPE_PARENT = "PARENT";
	private static final String STUDY_TYPE_TOPMED = "TOPMED";
	private static final String STUDY_TYPE_SUBSTUDY = "SUBSTUDY";

	// Harmonization constants
	private static final String HARMONIZED_YES = "Yes";
	private static final String HARMONIZED_NO = "No";

	// File pattern constants
	private static final String SUBJECT_MULTI_PATTERN = "subject.multi";
	private static final String FILE_EXTENSION_TXT = ".txt";

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

	/**
	 * Studies excluded from harmonized consent generation.
	 *
	 * <p>Exclusion Rationale:</p>
	 * <ul>
	 *   <li><b>MAYOVTE</b>: Excluded due to data quality issues identified during
	 *       initial harmonization validation. Study data structure does not conform
	 *       to harmonization requirements.</li>
	 * </ul>
	 *
	 * <p><b>Maintenance Note:</b> To add/remove studies from this list, consult with
	 * the BDC data curation team. Each exclusion should be documented with rationale.</p>
	 */
	private static List<String> HARMONIZE_OMISSION = new ArrayList<String>();
	static {
		HARMONIZE_OMISSION.add("MAYOVTE");
 	}

	// Note: HARMONIZED_HPDS_IDS removed per 2025-12-19 audit - was loaded but never used

	static List<String[]> _harmonized_consents = new ArrayList<>();
	
	
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
			
			e.printStackTrace();
		}

	}
	
	private static void execute() throws IOException {
		// read the spread sheet into a List<String[]>
		// Once GOOGLE API is ready to go replace this method with the google sheet methodology.
		List<BDCManagedInput> managedInputs = getManagedInputs();

		Map<String,Map<String,String>> patientMappings = getPatientMappings();



		
		Map<String,List<String[]>> consents = generateConsents(managedInputs, patientMappings);
				

		
		if(consents.containsKey(STUDY_TYPE_PARENT)) {
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "parent_consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(String[] line: consents.get(STUDY_TYPE_PARENT)) {
					writer.write(toCsv(line));
				}
			}
		}
		if(consents.containsKey(STUDY_TYPE_TOPMED)) {
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "topmed_consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for(String[] line: consents.get(STUDY_TYPE_TOPMED)) {
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
			Map<String,Map<String,String>> patientMappings) throws IOException {
		
		Map<String,List<String[]>> consents = new HashMap<>();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "consents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

		for(BDCManagedInput managedInput: managedInputs) {
			
			String studyType = managedInput.getStudyType().toUpperCase();
			
			String studyAbvName = managedInput.getStudyAbvName();
			
			String studyIdentifier = managedInput.getStudyIdentifier();

			Boolean isCompliant = managedInput.hasSubjectMultiFile();
			
			System.out.println("Building Consents for " + studyAbvName + " - " + studyIdentifier + " - " + studyType);
			
			List<String[]> studyConsents = buildConsents(studyIdentifier,studyAbvName,patientMappings);

			if(consents.containsKey(studyType)) {
				consents.get(studyType).addAll(studyConsents);
			} else {
				consents.put(studyType.toUpperCase(), new ArrayList<String[]>(studyConsents));
			}

			if(isCompliant) {
				for(String[] line: studyConsents){
					writer.write(toCsv(line));
				}
			}

			// Check harmonization exclusion list (see HARMONIZE_OMISSION JavaDoc for rationale)
			if(managedInput.getIsHarmonized().equalsIgnoreCase(HARMONIZED_YES) && !HARMONIZE_OMISSION.contains(studyAbvName.toUpperCase())) {
                System.out.println("Adding harmonized consents for " + studyAbvName);
			    _harmonized_consents.addAll(studyConsents);
                System.out.println("Harmonized list is now " + _harmonized_consents.size() + " elements long");
			}
		}
	}
		return consents;
	}


	private static List<String[]> buildConsents(String studyIdentifier, String studyAbvName, Map<String, Map<String, String>> patientMappings) throws IOException {
		File dataDir = new File(DATA_DIR);
		
		List<String[]> returnSet = new ArrayList<>();
		
		if(dataDir.isDirectory()) {
			
			String[] fileNames = dataDir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith(studyIdentifier) && name.toLowerCase().contains(SUBJECT_MULTI_PATTERN) && name.toLowerCase().endsWith(FILE_EXTENSION_TXT)) {
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
				try (CSVReader reader = new CSVReader(buffer, '\t', 'π')) {
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
					x = 0;
					if(consentidx == -1) {
						System.out.println("looking for consent header");
						
						for(String header: headers) {
							if(header.toLowerCase().contains("consent")) {
								consentidx = x;
								System.out.println("Found consent header " + header);
								break;
							}
							x++;
							
						}
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
						throw new IOException("Cannot find header for " + fileNames[0], new Throwable().fillInStackTrace());
					}
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
