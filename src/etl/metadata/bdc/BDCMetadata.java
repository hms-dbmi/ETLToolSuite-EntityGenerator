package etl.metadata.bdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.csv.bdc.BDCJob;
import etl.jobs.csv.bdc.DataDictionaryReader;
import etl.metadata.Metadata;

public class BDCMetadata implements Metadata {

	public List<BDCMetadataElements> bio_data_catalyst = new ArrayList<>();
	
	public static String REQEUST_ACCESS_LINK = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=";

	public static List<String> STATIC_META = new ArrayList<String>() {{
		add("ORCHID");
	}};
	
	private static List<String> CONSENT_HEADERS = new ArrayList<String>()
	{{
		
		add("consent".toLowerCase());
		add("consent_1217".toLowerCase());
		add("consentcol");
		add("gencons");
		add("Consent");
		
	}};
	@Deprecated
	public BDCMetadata(List<ManagedInput> managedInputs) throws IOException {
		
		Map<String,Map<String,String>> consentGroups = getConsentGroups(managedInputs);		
		
		for(ManagedInput _managedInput: managedInputs) {
			if(STATIC_META.contains(_managedInput.getStudyAbvName())) {
				System.out.println("Skipping static metadata: " + _managedInput.getStudyAbvName());
				continue;
			}
			if(!(_managedInput instanceof BDCManagedInput)) {
				continue;
			}
			BDCManagedInput managedInput = (BDCManagedInput) _managedInput;
			
			if(managedInput.getStudyAbvName().toUpperCase().equalsIgnoreCase("STUDY ABBREVIATED NAME")) {
				continue;
			}
			
			if(consentGroups.containsKey(managedInput.getStudyAbvName())) {
				
				int clinicalCount = getClinicalVariableCount(managedInput);
				
				for(Entry<String, String> entry: consentGroups.get(managedInput.getStudyAbvName()).entrySet()) {
					
					BDCMetadataElements bdcm = new BDCMetadataElements();
					
					bdcm.study_identifier = managedInput.getStudyIdentifier();
					
					bdcm.study_type = managedInput.getStudyType();
				
					bdcm.abbreviated_name = managedInput.getStudyAbvName();
				
					bdcm.full_study_name = managedInput.getStudyFullName();
					
					bdcm.consent_group_code = "c" + entry.getKey();
				
					bdcm.consent_group_name = entry.getValue();
										
					bdcm.consent_group_name_abv = entry.getValue().replaceAll(".*\\(","").replaceAll("\\).*", "").trim();
					
					bdcm.request_access = REQEUST_ACCESS_LINK + bdcm.study_identifier;
					
					bdcm.raw_clinical_variable_count = clinicalCount;
							
					getCounts(bdcm, managedInput, entry.getKey());
					
					if(bdcm.genetic_sample_size == 0 && bdcm.clinical_sample_size == 0) {
						
						bdcm.data_type = "";
						
					} else if(bdcm.genetic_sample_size == 0 && bdcm.clinical_sample_size > 0) {
						
						bdcm.data_type = "P";
						
					} else if(bdcm.genetic_sample_size > 0 && bdcm.clinical_variable_count <= 0) {
						
						bdcm.data_type = "G";
					
					} else if(bdcm.genetic_sample_size > 0 && bdcm.clinical_variable_count > 0) {
						bdcm.data_type = "P/G";
					}

					bdcm.study_version = BDCJob.getVersion(managedInput);
				
					bdcm.study_phase = BDCJob.getPhase(managedInput);
				
					bdcm.top_level_path = "\\" + bdcm.full_study_name + " ( " + bdcm.study_identifier + " )" + "\\";
				
					bdcm.is_harmonized = managedInput.getIsHarmonized();
				
					this.bio_data_catalyst.add(bdcm);	
				}
			} else {
				System.err.println("NO CONSENT GROUPS FOUND FOR " + managedInput.getStudyAbvName());
				
				addMissingConsents(managedInput);
				
			}

		}
	}
	
	public BDCMetadata(List<ManagedInput> managedInputs, File metadata) throws IOException {
	Map<String,Map<String,String>> consentGroups = getConsentGroups(managedInputs);		
	
	ObjectMapper mapper = new ObjectMapper();
	
	BDCMetadata bdcmeta = metadata.exists() ? mapper.readValue(metadata, BDCMetadata.class): new BDCMetadata();
	
	this.bio_data_catalyst.addAll(bdcmeta.bio_data_catalyst);
		
	for(ManagedInput _managedInput: managedInputs) {
		
		System.out.println(_managedInput);
		if(STATIC_META.contains(_managedInput.getStudyAbvName())) {
			System.out.println("Skipping static metadata: " + _managedInput.getStudyAbvName());
			continue;
		}
		if(!(_managedInput instanceof BDCManagedInput)) {
			continue;
		}
		BDCManagedInput managedInput = (BDCManagedInput) _managedInput;
		
		if(managedInput.getStudyAbvName().toUpperCase().equalsIgnoreCase("STUDY ABBREVIATED NAME")) {
			continue;
		}
		if(managedInput.getReadyToProcess().toUpperCase().startsWith("N")) {
			continue;
		}
		if(consentGroups.containsKey(managedInput.getStudyIdentifier())) {
			
			int clinicalCount = getClinicalVariableCount(managedInput);
		
			for(Entry<String, String> entry: consentGroups.get(managedInput.getStudyIdentifier()).entrySet()) {
					
					BDCMetadataElements bdcm = new BDCMetadataElements();
					
					bdcm.study_identifier = managedInput.getStudyIdentifier();
					
					bdcm.study_type = managedInput.getStudyType();
				
					bdcm.abbreviated_name = managedInput.getStudyAbvName();
				
					bdcm.full_study_name = managedInput.getStudyFullName();
					
					bdcm.consent_group_code = "c" + entry.getKey();
				
					bdcm.consent_group_name = entry.getValue();
					
					bdcm.consent_group_name_abv = entry.getValue().replaceAll(".*\\(","").replaceAll("\\).*", "").trim();

					bdcm.request_access = REQEUST_ACCESS_LINK + bdcm.study_identifier;
					
					bdcm.raw_clinical_variable_count = clinicalCount;
							
					getCounts(bdcm, managedInput, entry.getKey());
					
					if(bdcm.raw_clinical_sample_size == -1) {
						System.out.println(managedInput.getStudyIdentifier() + "." + bdcm.consent_group_code + " has no participants.");
						continue;
						
					}
					
					bdcm.data_type = managedInput.getDataType();
					/*
					// Old methodology for data type
					if(bdcm.genetic_sample_size == 0 && bdcm.clinical_sample_size == 0) {
						
						bdcm.data_type = "";
						
					} else if(bdcm.genetic_sample_size == 0 && bdcm.clinical_sample_size > 0) {
						
						bdcm.data_type = "P";
						
					} else if(bdcm.genetic_sample_size > 0 && bdcm.clinical_variable_count <= 0) {
						
						bdcm.data_type = "G";
					
					} else if(bdcm.genetic_sample_size > 0 && bdcm.clinical_variable_count > 0) {
						bdcm.data_type = "P/G";
					}
					*/
					bdcm.study_version = BDCJob.getVersion(managedInput);
				
					bdcm.study_phase = BDCJob.getPhase(managedInput);
				
					bdcm.top_level_path = "\\" + bdcm.full_study_name + " ( " + bdcm.study_identifier + " )" + "\\";
				
					bdcm.is_harmonized = managedInput.getIsHarmonized();
					
					if(this.bio_data_catalyst.contains(bdcm)) {
						System.out.println("replacing " + bdcm);
						this.bio_data_catalyst.remove(bdcm);
					}
					
					this.bio_data_catalyst.add(bdcm);	
				}
			} else {
				System.err.println("NO CONSENT GROUPS FOUND FOR " + managedInput.getStudyAbvName());
				
				addMissingConsents(managedInput);
				
			}
	
		}	
	}
	
	private int getClinicalVariableCount(BDCManagedInput managedInput) throws IOException {
		Set<String> finishedDataSets = new HashSet<>();
		
		Integer count = BDCJob.getVariableCountFromRawData(managedInput);

		return count;
	}


	private void getCounts(BDCMetadataElements bdcm, BDCManagedInput managedInput, String consent_group_code) throws IOException {
		
		String subjectFileName =  BDCJob.getStudySubjectMultiFile(managedInput);
		
		List<String> subjectFilePatientSet = new ArrayList<>();
		
		List<String> sampleFilePatientSet = new ArrayList<>();
		
		if(subjectFileName != null && !subjectFileName.isEmpty()) {
			subjectFilePatientSet = BDCJob.getPatientSetForConsentFromRawData(subjectFileName, managedInput, CONSENT_HEADERS, consent_group_code);
			
			bdcm.raw_clinical_sample_size = subjectFilePatientSet.size();
		} else {
			bdcm.raw_clinical_sample_size = -1;
		}

		String sampleFileName = BDCJob.getStudySampleMultiFile(managedInput);
		
		if(sampleFileName != null && !sampleFileName.isEmpty()) {

			sampleFilePatientSet = BDCJob.getPatientSetFromSampleFile(sampleFileName,managedInput);
		
		}
		
		Set<String> filteredSampleSet = sampleFilePatientSet.stream()
				.distinct()
				.filter(subjectFilePatientSet::contains)
				.collect(Collectors.toSet());
		
		bdcm.raw_genetic_sample_size = filteredSampleSet.size();
		
	}


	private Map<String, Map<String, String>> getConsentGroups(List<ManagedInput> managedInputs) throws IOException {
		
		Map<String, Map<String, String>> consentGroups = new HashMap<>();
		
		for(ManagedInput _managedInput: managedInputs) {
		
			BDCManagedInput managedInput = (BDCManagedInput) _managedInput;
			
			String subjectMultiFile = BDCJob.getStudySubjectMultiFile(managedInput);
			
			if(subjectMultiFile == null || subjectMultiFile.isEmpty()) {
				System.err.println("Missing subject multi file for " + managedInput.getStudyAbvName() + ":" + managedInput.getStudyIdentifier());
				continue;
			}
			
			File subjectDataDict = BDCJob.FindDictionaryFile(subjectMultiFile);
			
			Map<String, Map<String, String>> valueLookup = DataDictionaryReader.buildValueLookup(subjectDataDict);

			// Does a key contain a valid consent
			for(String consentHeader: CONSENT_HEADERS) {
				
				if(valueLookup.containsKey(consentHeader.toUpperCase())) {
					consentGroups.put(managedInput.getStudyIdentifier(), valueLookup.get(consentHeader.toUpperCase()));
					break;
				} else if(valueLookup.containsKey(consentHeader)) {
					consentGroups.put(managedInput.getStudyIdentifier(), valueLookup.get(consentHeader));
					break;
				}
				
			}
		
		}
		
		return consentGroups;
	}


	private void addMissingConsents(BDCManagedInput managedInput) {
		
		BDCMetadataElements bdcm = new BDCMetadataElements();
		
		bdcm.study_identifier = managedInput.getStudyIdentifier();
		
		bdcm.study_type = managedInput.getStudyType();
	
		bdcm.abbreviated_name = managedInput.getStudyAbvName();
	
		bdcm.full_study_name = managedInput.getStudyFullName();
		
		bdcm.consent_group_code = "MISSING CONSENTS INFORMATION WHILE BUILDING METADATA";
	
		bdcm.consent_group_name = "MISSING CONSENTS INFORMATION WHILE BUILDING METADATA";
	
		bdcm.request_access = "";
	
		bdcm.data_type = managedInput.getDataType();
	
		bdcm.clinical_variable_count = -1;
	
		bdcm.genetic_sample_size = -1;
	
		bdcm.clinical_sample_size = -1;
	
		bdcm.study_version = "";
	
		bdcm.study_phase = "";
	
		bdcm.top_level_path = "\\" + bdcm.full_study_name + " ( " + bdcm.study_identifier + " )" + "\\";
	
		bdcm.is_harmonized = managedInput.getIsHarmonized();
		if(!this.bio_data_catalyst.contains(bdcm)) {
			this.bio_data_catalyst.add(bdcm);		
		}
	}


	public BDCMetadata() {
		// TODO Auto-generated constructor stub
	}

	public static BDCMetadata readMetadata(File file) {
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			BDCMetadata meta = mapper.readValue(file, BDCMetadata.class);
			return meta;

		} catch (IOException e) {
			System.err.println("ERROR READING METADATA FILE");
			e.printStackTrace();
		
		
		}
		return null;
	}

	
}
