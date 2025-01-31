package etl.metadata.bdc;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

public class GenericBDCMetadata extends BDCMetadata {
	public static String GENERIC_CONSENT_GROUP_FULL_NAME = "";
	
	public static String GENERIC_CONSENT_GROUP_ABV_NAME = "";
	
	public static String GENERIC_CONSENT_CODE = "";

	public static String STUDY_ACCESSION = "";
	
	public GenericBDCMetadata(List<BDCManagedInput> managedInputs, File metadata) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		
		BDCMetadata bdcmeta = metadata.exists() ? mapper.readValue(metadata, BDCMetadata.class): new BDCMetadata();
		
		BDCManagedInput managedInput = findManagedInput(managedInputs);
		
		this.bio_data_catalyst.addAll(bdcmeta.bio_data_catalyst);
		// expect consents to have values
		
		if(managedInput == null) {
			throw new IOException("Unable to find accession in Managed Input for " + STUDY_ACCESSION);
		}
/* 		if(StringUtils.isBlank(GENERIC_CONSENT_CODE)) {
			throw new IOException("GENERIC_CONSENT_CODE is not set");
		}
		if(StringUtils.isBlank(GENERIC_CONSENT_GROUP_ABV_NAME)) {
			throw new IOException("GENERIC_CONSENT_GROUP_ABV_NAME is not set");
		}
		if(StringUtils.isBlank(GENERIC_CONSENT_GROUP_FULL_NAME)) {
			throw new IOException("GENERIC_CONSENT_GROUP_FULL_NAME is not set");
		} */
		
		BDCMetadataElements bdcm = new BDCMetadataElements();
		
		bdcm.study_identifier = managedInput.getStudyIdentifier();
		
		bdcm.study_type = managedInput.getStudyType();
	
		bdcm.abbreviated_name = managedInput.getStudyAbvName();
	
		bdcm.full_study_name = managedInput.getStudyFullName();
		
		bdcm.consent_group_code = GENERIC_CONSENT_CODE;
	
		bdcm.consent_group_name = GENERIC_CONSENT_GROUP_FULL_NAME;
		
		bdcm.consent_group_name_abv = GENERIC_CONSENT_GROUP_ABV_NAME;

		bdcm.request_access = REQEUST_ACCESS_LINK + bdcm.study_identifier;
		
		bdcm.raw_clinical_variable_count = -1;
		
		bdcm.clinical_variable_count = -1;
		
		//getCounts(bdcm, managedInput, entry.getKey());
		
		bdcm.data_type = managedInput.getDataType();
		
		bdcm.study_version = managedInput.getVersion();
	
		bdcm.study_phase = managedInput.getPhase();
	
		bdcm.top_level_path = "\\" + bdcm.study_identifier + "\\";
	
		bdcm.is_harmonized = managedInput.getIsHarmonized();

		if (managedInput.getAuthZ().endsWith("_")) {
			bdcm.authZ = managedInput.getAuthZ() + bdcm.consent_group_name_abv;
		} else {
			bdcm.authZ = managedInput.getAuthZ();
		}
		
		if(this.bio_data_catalyst.contains(bdcm)) {
			System.out.println("replacing " + bdcm);
			this.bio_data_catalyst.remove(bdcm);
			this.bio_data_catalyst.add(bdcm);
		} else {
			System.out.println("adding " + bdcm);
			this.bio_data_catalyst.add(bdcm);
		}
		
	}

	private BDCManagedInput findManagedInput(List<BDCManagedInput> managedInputs) {
		for(ManagedInput x: managedInputs) {
			BDCManagedInput mi = (BDCManagedInput) x;
			if(mi.getStudyIdentifier().equalsIgnoreCase(STUDY_ACCESSION)) return mi;
		}
		return null;
	}
	
}
