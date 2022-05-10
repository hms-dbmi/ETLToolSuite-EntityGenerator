package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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


/**
 * 
 * Purge any c0 patients from each studies allConcepts ( this will remove them from global vars as well ).
 * remove any patient records whose study is not loaded from HRMN_allConcepts and harmonized_consents
 * 
 * @author Tom
 *
 */
public class RemoveConsentZeroPatients extends BDCJob {

	private static final String GLOBAL_CONSENTS_PATH = "µ_consentsµ";
	private static final String HARMONIZED_CONSENTS_PATH = "\\_harmonized_consents\\";
	private static final boolean RUN_VALIDATION = false;

	private static List<BDCManagedInput> managedInputs;
	private static Set<String> topmedNonCzeroPatNums;
	private static Set<String> harmonizedValidPatientNums;
	private static Map<String, Set<String>> globalNonConsentZeroPatientNums;
	
	private static int acPreCount = 0;
	private static int acPostCount = 0;
	
	private static int totCZpat = 0;
	private static final String[] AC_HEADERS = new String[5];
	static {
		AC_HEADERS[0] = "PATIENT_NUM";
		AC_HEADERS[1] = "CONCEPT_PATH";
		AC_HEADERS[2] = "NVAL_NUM";
		AC_HEADERS[3] = "TVAL_CHAR";
		AC_HEADERS[4] = "DATE_TIME";
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
			
			System.err.println(e);
			
		}
	}

	private static void execute() throws IOException {

		managedInputs = getManagedInputs();

		globalNonConsentZeroPatientNums = readConsents(GLOBAL_CONSENTS_PATH);
		
		System.out.println(globalNonConsentZeroPatientNums.size());
		// build a hash set with only harmonized 
		
		harmonizedValidPatientNums = buildHarmonizedValidPatNums();
		
		System.out.println(harmonizedValidPatientNums.size());

		topmedNonCzeroPatNums = buildTopmedPatNums();
		
		purgePatients();
		
		if(RUN_VALIDATION) {
			validate();
		}
	}

	private static void validate() throws IOException {
		Set<String> set = new HashSet<>();
		try(BufferedReader br = Files.newBufferedReader(Paths.get(WRITE_DIR + "allConcepts.csv"))){
			
			CSVReader csvreader = new CSVReader(br, ',', '\"', 'µ');

			String[] line;
			
			while((line = csvreader.readNext())!=null) {
				set.add(line[0]);
			}
						
		};
		acPreCount = set.size();
		
		set = new HashSet<>();

		try(BufferedReader br = Files.newBufferedReader(Paths.get(PROCESSING_FOLDER + "allConcepts.csv"))){
			
			CSVReader csvreader = new CSVReader(br, ',', '\"', 'µ');

			String[] line;
			
			while((line = csvreader.readNext())!=null) {
				set.add(line[0]);
			}
						
		};
		
		acPostCount = set.size();
		
		System.out.println("Pre count = " + acPreCount);
		
		System.out.println("Post count = " + acPostCount);
		
		System.out.println("Diff count = " + (acPreCount - acPostCount));
		
		System.out.println("Expected diff = " + totCZpat);

	}

	private static Set<String> buildTopmedPatNums() {
		Set<String> set = new HashSet<>();	
		for(BDCManagedInput input: managedInputs) {
			
			if(input.getStudyType().trim().equalsIgnoreCase("TOPMED")) {
				if(globalNonConsentZeroPatientNums.containsKey(input.getStudyIdentifier())) {
					set.addAll(globalNonConsentZeroPatientNums.get(input.getStudyIdentifier()));
				} else {
					System.err.println("Missing Topmed patients from " + input.getStudyIdentifier());

				}
				
			}
			
		}
		
		return set;
	}

	private static Set<String> buildHarmonizedValidPatNums() {
		Set<String> set = new HashSet<>();	
		for(BDCManagedInput input: managedInputs) {
			
			if(input.getIsHarmonized().equalsIgnoreCase("Y")) {
				if(globalNonConsentZeroPatientNums.containsKey(input.getStudyIdentifier())) {
					set.addAll(globalNonConsentZeroPatientNums.get(input.getStudyIdentifier()));
				} else {
					System.err.println("Missing harmonized patients from " + input.getStudyIdentifier());
				}
			}
			
		}
		
		return set;
	}

	private static void purgePatients() throws IOException {
		 
		 Map<String,String> phsLookup = new HashMap<>();
		 
		 for(BDCManagedInput input: managedInputs) {
			//String fullname = input.getStudyFullName() + " ( " + input.getStudyIdentifier() + " ) ";

			phsLookup.put(input.getStudyIdentifier(), input.getStudyIdentifier());
		 }
		 
		try(BufferedWriter bw = Files.newBufferedWriter(Paths.get(PROCESSING_FOLDER + "allConcepts.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)){
			String line[];
			
			BufferedReader br = Files.newBufferedReader(Paths.get(WRITE_DIR + "allConcepts.csv"));
			bw.write(BDCJob.toCsv(AC_HEADERS));

			CSVReader csvreader = new CSVReader(br, ',', '\"', 'µ');
			int x = 0;
			while((line = csvreader.readNext())!=null) {
				
				if(line[1].split("\\\\").length <=1) continue;
				// lets get rid of those line breaks in the data
				line[3] = line[3].replaceAll("\n", "");
				String rootNode = line[1].split("\\\\")[1].trim();
				
				if(rootNode.equals("_studies")) {
					rootNode = line[1].split("\\\\")[2].trim();
				}
				String phsIdentifier = phsLookup.containsKey(rootNode) ? phsLookup.get(rootNode) : null;
				
				if(rootNode.equals("_Topmed Study Accession with Subject ID") || rootNode.equals("_Parent Study Accession with Subject ID")) {
					phsIdentifier = line[3].split("\\.")[0];
				}
				
				if(phsIdentifier == null) {
					if(rootNode.equals("DCC Harmonized data set")) {
						if(harmonizedValidPatientNums.contains(line[0])) {
							
							bw.write(toCsv(line));
						}
					}
					if(rootNode.equals("_VCF Sample Id")) {
						if(topmedNonCzeroPatNums.contains(line[0])) {
							
							bw.write(toCsv(line));
						}					
					} else if(rootNode.equals("_topmed_consents")) { 
						if(line[3].contains("c0")) continue;
						bw.write(toCsv(line));
					} else if(rootNode.equals("_parent_consents")) { 
						if(line[3].contains("c0")) continue;
						
						bw.write(toCsv(line));
					} else if(rootNode.equals("_consents")) {
						if(line[3].contains("c0")) continue;
						
						bw.write(toCsv(line));
						
					} else if(rootNode.equals("_studies_consents")) {
						
						bw.write(toCsv(line));
						
					} else if(rootNode.equals("_harmonized_consent")) {
						if(line[3].contains("c0")) continue;
						
						bw.write(toCsv(line));
					}
				} else {
					if(globalNonConsentZeroPatientNums.containsKey(phsIdentifier.trim())) {
						if(globalNonConsentZeroPatientNums.get(phsIdentifier).contains(line[0])) {
							
							bw.write(toCsv(line));
							
						}
							
					}
				}
				
				bw.flush();
			}
		}
		
	}

	private static Map<String,Set<String>> readConsents(String globalConsentsPath) throws IOException {
		
		Map<String,Set<String>> patnums = new HashMap<String,Set<String>>();
		Set<String> c0 = new HashSet<>();
		Set<String> cGood = new HashSet<>();
 		try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))) {
			CSVReader csvreader = new CSVReader(reader);
			
			String[] line;
			while((line = csvreader.readNext()) != null) {
				if(line[1].equals(GLOBAL_CONSENTS_PATH)) {
					line[0] = line[0].trim();
					if(line[3].contains("c0")) {
						c0.add(line[0].trim());
						continue;
					};  // skip c0
					if(patnums.containsKey(line[3].replaceAll("\\.c[0-9]", "").trim())) {
						//System.out.println("adding to " + line[3].replaceAll("\\.c[0-9]", ""));
						patnums.get(line[3].replaceAll("\\.c[0-9]", "")).add(line[0]);
						cGood.add(line[0]);
						
						
					} else {
						Set<String> set = new HashSet<>();
						set.add(line[0].trim());
						//System.out.println("adding to " + line[3].replaceAll("\\.c[0-9]", ""));
						patnums.put(line[3].replaceAll("\\.c[0-9]", "").trim(), set);
						cGood.add(line[0]);
						
					}
					
				}
			}
			for(String c: cGood) {
				if(c0.contains(c)) c0.remove(c);
			}
			totCZpat = c0.size();
			return patnums;
		}
	}
}