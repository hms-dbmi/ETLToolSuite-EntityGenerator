package etl.job.entity.i2b2tm;

import com.opencsv.bean.CsvBindByPosition;

public class PatientTrial {
	@CsvBindByPosition(position = 0)
	private String patientNum;
	@CsvBindByPosition(position = 1)
	private String trial;
	@CsvBindByPosition(position = 2)
	private String secureObjectToken = "EXP:PUBLIC";
	
	public PatientTrial() {
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
