package etl.metadata.bdc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
		add("HCT_for_SCD");
	}};
	
	private static List<String> CONSENT_HEADERS = new ArrayList<String>()
	{{
		
		add("consent".toLowerCase());
		add("consent_1217".toLowerCase());
		add("consentcol");
		add("gencons");
		add("Consent");
		
	}};
	
	
	public BDCMetadata(List<BDCManagedInput> managedInputs, File metadata) throws IOException {
	Map<String,Map<String,String>> consentGroups = getConsentGroups(managedInputs);		
	
	ObjectMapper mapper = new ObjectMapper();
	
	BDCMetadata bdcmeta = metadata.exists() ? mapper.readValue(metadata, BDCMetadata.class): new BDCMetadata();
	
	this.bio_data_catalyst.addAll(bdcmeta.bio_data_catalyst);
		
	for(ManagedInput _managedInput: managedInputs) {
		
		System.out.println(_managedInput);
		/*if(STATIC_META.contains(_managedInput.getStudyAbvName())) {
			System.out.println("Skipping static metadata: " + _managedInput.getStudyAbvName());
			continue;
		}*/
		if(!(_managedInput instanceof BDCManagedInput)) {
			continue;
		}
		BDCManagedInput managedInput = (BDCManagedInput) _managedInput;
		
		/*
		if(!managedInput.getStudyType().equalsIgnoreCase("TOPMED") && !managedInput.getStudyType().equalsIgnoreCase("PARENT")) {
			buildGenericMetadata(managedInput,metadata);
			continue;
		}*/
		if(managedInput.getStudyAbvName().toUpperCase().equalsIgnoreCase("STUDY ABBREVIATED NAME")) {
			continue;
		}
		if(managedInput.getReadyToProcess().toUpperCase().startsWith("N")) {
			continue;
		}
		if(managedInput.getDataProcessed().toUpperCase().startsWith("Y")) {
			continue;
		}
		if (!managedInput.hasSubjectMultiFile()) {
			continue;
		}
		if(consentGroups.containsKey(managedInput.getStudyIdentifier())) {
			
			System.out.println("Building metadata for " + managedInput.toString());
			for(Entry<String, String> entry: consentGroups.get(managedInput.getStudyIdentifier()).entrySet()) {
					
					BDCMetadataElements bdcm = new BDCMetadataElements();
					
					bdcm.study_identifier = managedInput.getStudyIdentifier();
					
					bdcm.study_type = managedInput.getStudyType();
				
					bdcm.abbreviated_name = managedInput.getStudyAbvName();
				
					bdcm.full_study_name = managedInput.getStudyFullName();
					
					bdcm.consent_group_code = "c" + entry.getKey();
					if (bdcm.consent_group_code.equals("c0")){
						System.out.println("Skipping metadata for c0 participants");
						continue;
					}
				
					bdcm.consent_group_name = entry.getValue();
					
					bdcm.consent_group_name_abv = entry.getValue().replaceAll(".*\\(","").replaceAll("\\).*", "").trim();

					bdcm.request_access = REQEUST_ACCESS_LINK + bdcm.study_identifier;
					
					
					//builds the authZ value from the relevant components if needed
					if(managedInput.getAuthZ().endsWith("_")){
						bdcm.authZ = managedInput.getAuthZ()+bdcm.consent_group_name_abv;
					}
					else{
						bdcm.authZ = managedInput.getAuthZ();
					}
					if(managedInput.getStudyType().equalsIgnoreCase("parent")){
						bdcm.authZ = bdcm.authZ + "_";
					}
					
										
					bdcm.data_type = managedInput.getDataType();
				
					bdcm.study_version = managedInput.getVersion();
				
					bdcm.study_phase = managedInput.getPhase();
				
					bdcm.top_level_path = "\\" + bdcm.study_identifier + "\\";
				
					bdcm.is_harmonized = managedInput.getIsHarmonized();
					
					if(this.bio_data_catalyst.contains(bdcm)) {
						System.out.println("replacing " + bdcm);
						this.bio_data_catalyst.remove(bdcm);
					}
					
					this.bio_data_catalyst.add(bdcm);	
				}
			} else {
				System.err.println("NO CONSENT GROUPS FOUND FOR " + managedInput.getStudyIdentifier() + ": " + managedInput.getStudyAbvName());
				System.exit(-1);
			}
	
		}	
	}
	



	


	private Map<String, Map<String, String>> getConsentGroups(List<BDCManagedInput> managedInputs) throws IOException {
		
		Map<String, Map<String, String>> consentGroups = new HashMap<>();
		
		for(ManagedInput _managedInput: managedInputs) {
			
			BDCManagedInput managedInput = (BDCManagedInput) _managedInput;
			
			if(!managedInput.hasSubjectMultiFile()) {
				continue;
			}
			
			String subjectMultiFile = BDCJob.getStudySubjectMultiFile(managedInput);
			
			if(subjectMultiFile == null || subjectMultiFile.isEmpty()) {
				System.err.println("Missing subject multi file for " + managedInput.getStudyAbvName() + ":" + managedInput.getStudyIdentifier());
				continue;
			}
			
			File subjectDataDict = BDCJob.FindDictionaryFile(subjectMultiFile, BDCJob.DATA_DIR + "raw/" );
			
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
			// if no consent header found look dynamically
			if(!consentGroups.containsKey(managedInput.getStudyIdentifier())) {
				System.out.println("searching dynamically for consent header...");
				for(String key: valueLookup.keySet()) {
					if(key.toUpperCase().contains("CONSENT")) {
						consentGroups.put(managedInput.getStudyIdentifier(), valueLookup.get(key));
						
						System.out.println("consent header detected = " + key);
						
						CONSENT_HEADERS.add(key);
						break;
					}
				}
			}
		}
		
		return consentGroups;
	}

	public BDCMetadata() {
		
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
