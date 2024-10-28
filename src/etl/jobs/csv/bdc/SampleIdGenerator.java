package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVParser;
import com.opencsv.CSVWriter;

import etl.etlinputs.managedinputs.bdc.BDCGenomicManagedInput;

public class SampleIdGenerator extends BDCJob {

	private static List<String> SAMPLE_HEADERS = new ArrayList<String>();
	static {
		
		SAMPLE_HEADERS.add("SAMPLE_ID");
		SAMPLE_HEADERS.add("SAMPLEID");
		SAMPLE_HEADERS.add("SAMPID");

	}
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
		//cleanSampleIdFile();
		List<BDCGenomicManagedInput> managedInputs = BDCJob.getGenomicManagedInputs();
		for (BDCGenomicManagedInput managedInput : managedInputs) {
				buildSampleIds(managedInput.getStudyIdAndConsent(), managedInput.getGlobalConsentCode(), managedInput.getStudyIdentifier(), managedInput.getStudyFreeze());
		}
	}

	private static void buildSampleIds(String fullId, String globalConsentCode, String phsNum, String freeze) throws IOException {
		
		File dataDir = new File(DATA_DIR);
		
		List<String> patientNums = new ArrayList<>();
		List<String> sampleIds = new ArrayList<>();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + fullId + "_SampleIds.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
		
			if(dataDir.isDirectory()) {
				
				for(String f: dataDir.list()) {
										
					if(f.toLowerCase().contains(phsNum+".sample.multi")) {
						
						
						String studyAbv = getStudyAccessions(fullId);
						
						if(studyAbv == null || studyAbv.isEmpty()) {
							System.err.println("No study associated with " + fullId + " in genomic managed input file");
							continue;
						};
						
						try(BufferedReader buffer = Files.newBufferedReader(Paths.get(new File(DATA_DIR + f).getAbsolutePath()))) {
							
							Map<String,String> patientNumLookup = getPatientMapping(studyAbv);

							Map<String,String> consentMap = getConsentMappings();
							
							List<String> headers2 = new ArrayList<String>();
							
							CSVReader reader = new CSVReader(buffer, '\t');
	
							String[] line;
	
							int x = 0;
							int sampidIdx = -1;
							while((line = reader.readNext()) != null) { 
	
								if(!line[0].toUpperCase().contains("DBGAP")) continue;
								
								for(String col: line) {
									if(SAMPLE_HEADERS.contains(col.toUpperCase())) {
										sampidIdx = x;
										break;
									} else {
										x++;
									}
								}
								if(line[0].toUpperCase().contains("DBGAP")) break;
								
							}
							
							if(sampidIdx == -1) {
								System.err.println("No sample column for " + fullId);
								continue;
							}
							
							line = null;
							
							while((line = reader.readNext()) != null) {
								
								if(patientNumLookup.containsKey(line[0])) {
									//checks if sampleid is empty
									if(line[sampidIdx].trim().isEmpty()) continue;
									//checks if sampleid fits the expected format
									if(!line[sampidIdx].startsWith("NWD")) continue;
									// checks if the patient belongs to the correct consent code for the index being generated
									boolean consentCheck;
									try{
										consentCheck = (consentMap.get(patientNumLookup.get(line[0])).equals(globalConsentCode));
									
									}
									catch(NullPointerException e){
										consentCheck =  false;
										System.out.println("Null pointer exception on " + fullId);
									}

									if(!consentCheck) continue;
									String[] arr = new String[] { patientNumLookup.get(line[0]), studyAbv, line[sampidIdx]};
									patientNums.add(patientNumLookup.get(line[0]).trim());
									sampleIds.add(line[sampidIdx].trim());
									writer.write(toCsv(arr));
									
								}
							}
							writer.flush();
						}
					}
				}
			}
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + fullId + "_vcfIndex.tsv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			
			CSVWriter csv = new CSVWriter(writer, '\t');
			String[] topText = new String[] {fullId};
			csv.writeNext(topText);
			String[] headers = new String[] {"vcf_path","chromosome","isAnnotated","isGzipped","sample_ids","patient_ids","sample_relationship","related_sample_ids"};
			csv.writeNext(headers);
			String isAnnotated = "1";
			String isGzipped = "1";
			for(int chrom = 1; chrom<24; chrom++){
				String chromosome = Integer.toString(chrom);
				if (chrom == 23){
					chromosome = "X";
				}
				String vcfPath = "data/vcfInput/" + fullId + ".chr" + chromosome + ".annotated_remove_modifiers.hpds.vcf.gz";
	
				String[] line = new String[] { vcfPath, chromosome, isAnnotated, isGzipped, String.join(",", sampleIds), String.join(",", patientNums) };

				csv.writeNext(line);
			}
			csv.flush();
			
		}

	}

	private static Map<String, String> getPatientMapping(String studyAbv) throws IOException {
		Map<String,String> map = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + studyAbv.toUpperCase() + "_PatientMapping.v2.csv"))){
			
			//System.out.println(studyId.toUpperCase() + "_PatientMapping.v2.csv");
			CSVReader reader = new CSVReader(buffer);
			
			String[] arr;
			
			while((arr = reader.readNext()) != null) {
				if(arr.length > 2) {
					map.put(arr[0], arr[2]);
				}
			}
		}
		return map;
	}

	private static String getStudyAccessions(String fullId) throws IOException {
		
		List<BDCGenomicManagedInput> managedInputs = BDCJob.getGenomicManagedInputs();

		
		for(BDCGenomicManagedInput managedInput:managedInputs) {
			if(managedInput.getStudyIdAndConsent().equals(fullId)) {
				return managedInput.getStudyAbvName();
			}
		}
		
		return null;
	}

	private static Map<String, String> getConsentMappings() throws IOException {
		Map<String, String> consentMap = new HashMap<>();

		try (BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts_merged.csv"))) {
			CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(true).withEscapeChar('µ').build();
			CSVReader reader = new CSVReaderBuilder(buffer).withCSVParser(parser).build();
			String[] line;

			String currentNode = "";

			while ((line = reader.readNext()) != null) {
				String rootConcept = line[1].split("\"")[0];

				if (rootConcept.equalsIgnoreCase("_topmed_consents")) {
					String consentGroup = line[3];
					consentMap.put(line[0], consentGroup);
				}

			}
			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(DATA_DIR + "Patient_Consent_Map.csv"),
					StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for (Entry<String, String> entry : consentMap.entrySet()) {
					writer.write(toCsv(new String[] { entry.getKey(), entry.getValue() }));
				}
			}
		}

		return consentMap;
	}

}
