package etl.data.export.entities.i2b2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;

public class PatientTrial extends Entity {
	private String patientNum;
	private String trial;
	private String secureObjectToken = "EXP:PUBLIC";
	
	public PatientTrial(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	public PatientTrial(String string, LinkedHashMap record, Map<String, List<Object>> valueMap) throws Exception {
		super(string);
		for(String fieldKey: valueMap.keySet()) {
			this.patientNum = fieldKey.equalsIgnoreCase("patientNum") && !valueMap.get(fieldKey).isEmpty() ? valueMap.get(fieldKey).get(0).toString() : this.patientNum;
			this.trial = SOURCESYSTEM_CD;
		}
	}

	public String getPatientNum() {
		return patientNum;
	}

	public void setPatientNum(String patientNum) {
		this.patientNum = patientNum;
	}

	public String getTrial() {
		return trial;
	}

	public void setTrial(String trial) {
		this.trial = trial;
	}

	public String getSecureObjectToken() {
		return secureObjectToken;
	}

	public void setSecureObjectToken(String secureObjectToken) {
		this.secureObjectToken = secureObjectToken;
	}

	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping, String[] data) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toCsv() {
		return 	makeStringSafe(patientNum) + "," +
				makeStringSafe(trial) + "," +
				makeStringSafe(secureObjectToken);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patientNum == null) ? 0 : patientNum.hashCode());
		result = prime * result + ((secureObjectToken == null) ? 0 : secureObjectToken.hashCode());
		result = prime * result + ((trial == null) ? 0 : trial.hashCode());
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
		PatientTrial other = (PatientTrial) obj;
		if (patientNum == null) {
			if (other.patientNum != null)
				return false;
		} else if (!patientNum.equals(other.patientNum))
			return false;
		if (secureObjectToken == null) {
			if (other.secureObjectToken != null)
				return false;
		} else if (!secureObjectToken.equals(other.secureObjectToken))
			return false;
		if (trial == null) {
			if (other.trial != null)
				return false;
		} else if (!trial.equals(other.trial))
			return false;
		return true;
	}
	
	

}
