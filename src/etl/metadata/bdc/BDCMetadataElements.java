package etl.metadata.bdc;

public class BDCMetadataElements {

	public String study_identifier = "";
	
	public String study_type = "";

	public String abbreviated_name = "";

	public String full_study_name = "";

	public String consent_group_code = "";

	public String consent_group_name_abv = "";	
	
	public String consent_group_name = "";

	public String request_access = "";

	public String data_type = "";

	public Integer clinical_variable_count = -1;

	public Integer genetic_sample_size = -1;

	public Integer clinical_sample_size = -1;

	public Integer raw_clinical_variable_count = -1;

	public Integer raw_genetic_sample_size = -1;

	public Integer raw_clinical_sample_size = -1;
	
	public String study_version = "";

	public String study_phase = "";

	public String top_level_path = "";

	public String is_harmonized = "";

	public String study_focus = "";

    public String study_design = "";
    public String additional_information = "";
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((consent_group_code == null) ? 0 : consent_group_code.hashCode());
		result = prime * result + ((study_identifier == null) ? 0 : study_identifier.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BDCMetadataElements other = (BDCMetadataElements) obj;
		if (consent_group_code == null) {
			if (other.consent_group_code != null)
				return false;
		} else if (!consent_group_code.equals(other.consent_group_code))
			return false;
		if (study_identifier == null) {
			if (other.study_identifier != null)
				return false;
		} else if (!study_identifier.equals(other.study_identifier))
			return false;
		return true;
	}

	
	
	
}
