package etl.job.entity.i2b2tm;

import java.util.ArrayList;
import java.util.List;

import com.opencsv.bean.CsvBindByPosition;

public class PatientDimension {
    @CsvBindByPosition(position = 0)
	private String patientNum;
    @CsvBindByPosition(position = 1)
	private String vitalStatusCD;
	@CsvBindByPosition(position = 2)
	private String birthDate;
	@CsvBindByPosition(position = 3)
	private String deathDate;
	@CsvBindByPosition(position = 4)
	private String sexCD;
	@CsvBindByPosition(position = 5)
	private String ageInYearsNum;
	@CsvBindByPosition(position = 6)
	private String languageCD;
	@CsvBindByPosition(position = 7)
	private String raceCD;
	@CsvBindByPosition(position = 8)
	private String maritalStatusCD;
	@CsvBindByPosition(position = 9)
	private String religionCD;
	@CsvBindByPosition(position = 10)
	private String zipCD;
	@CsvBindByPosition(position = 11)
	private String stateCityZipPath;
	@CsvBindByPosition(position = 12)
	private String updateDate;
	@CsvBindByPosition(position = 13)
	private String downloadDate;
	@CsvBindByPosition(position = 14)
	private String importDate;
	@CsvBindByPosition(position = 15)
	private String sourceSystemCD;
	@CsvBindByPosition(position = 16)
	private String uploadID;
	@CsvBindByPosition(position = 17)
	private String patientBlob;
	@CsvBindByPosition(position = 18)
	private String incomeCD;
	
	public PatientDimension() {

	}
	public PatientDimension(String[] arr) throws Exception {
		if(arr.length != 19) throw new Exception("Record missing columns!");
		this.patientNum = arr[0];
		this.vitalStatusCD = arr[1];
		this.birthDate = arr[2];
		this.deathDate = arr[3];
		this.sexCD = arr[4];
		this.ageInYearsNum = arr[5];
		this.languageCD = arr[6];
		this.raceCD = arr[7];
		this.maritalStatusCD = arr[8];
		this.religionCD = arr[9];
		this.zipCD = arr[10];
		this.stateCityZipPath = arr[11];
		this.updateDate = arr[12];
		this.downloadDate = arr[13];
		this.importDate = arr[14];
		this.sourceSystemCD = arr[15];
		this.uploadID = arr[16];
		this.patientBlob = arr[17];
		this.incomeCD = arr[18];
	}
	
	public boolean isValid() {
		return (this.patientNum != null && !this.patientNum.isEmpty()) ? true: false;
	}

	public String getPatientNum() {
		return patientNum;
	}

	public void setPatientNum(String patientNum) {
		this.patientNum = patientNum;
	}

	public String getVitalStatusCD() {
		return vitalStatusCD;
	}

	public void setVitalStatusCD(String vitalStatusCD) {
		this.vitalStatusCD = vitalStatusCD;
	}

	public String getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(String birthDate) {
		this.birthDate = birthDate;
	}

	public String getDeathDate() {
		return deathDate;
	}

	public void setDeathDate(String deathDate) {
		this.deathDate = deathDate;
	}

	public String getSexCD() {
		return sexCD;
	}

	public void setSexCD(String sexCD) {
		this.sexCD = sexCD;
	}

	public String getAgeInYearsNum() {
		return ageInYearsNum;
	}

	public void setAgeInYearsNum(String ageInYearsNum) {
		this.ageInYearsNum = ageInYearsNum;
	}

	public String getLanguageCD() {
		return languageCD;
	}

	public void setLanguageCD(String languageCD) {
		this.languageCD = languageCD;
	}

	public String getRaceCD() {
		return raceCD;
	}

	public void setRaceCD(String raceCD) {
		this.raceCD = raceCD;
	}

	public String getMaritalStatusCD() {
		return maritalStatusCD;
	}

	public void setMaritalStatusCD(String maritalStatusCD) {
		this.maritalStatusCD = maritalStatusCD;
	}

	public String getReligionCD() {
		return religionCD;
	}

	public void setReligionCD(String religionCD) {
		this.religionCD = religionCD;
	}

	public String getZipCD() {
		return zipCD;
	}

	public void setZipCD(String zipCD) {
		this.zipCD = zipCD;
	}

	public String getStateCityZipPath() {
		return stateCityZipPath;
	}

	public void setStateCityZipPath(String stateCityZipPath) {
		this.stateCityZipPath = stateCityZipPath;
	}

	public String getUpdateDate() {
		return updateDate;
	}

	public void setUpdateDate(String updateDate) {
		this.updateDate = updateDate;
	}

	public String getDownloadDate() {
		return downloadDate;
	}

	public void setDownloadDate(String downloadDate) {
		this.downloadDate = downloadDate;
	}

	public String getImportDate() {
		return importDate;
	}

	public void setImportDate(String importDate) {
		this.importDate = importDate;
	}

	public String getSourceSystemCD() {
		return sourceSystemCD;
	}

	public void setSourceSystemCD(String sourceSystemCD) {
		this.sourceSystemCD = sourceSystemCD;
	}

	public String getUploadID() {
		return uploadID;
	}

	public void setUploadID(String uploadID) {
		this.uploadID = uploadID;
	}

	public String getPatientBlob() {
		return patientBlob;
	}

	public void setPatientBlob(String patientBlob) {
		this.patientBlob = patientBlob;
	}

	public String getIncomeCD() {
		return incomeCD;
	}

	public void setIncomeCD(String incomeCD) {
		this.incomeCD = incomeCD;
	}

	@Override
	public String toString() {
		return "PatientDimension [patientNum=" + patientNum
				+ ", vitalStatusCD=" + vitalStatusCD + ", birthDate="
				+ birthDate + ", deathDate=" + deathDate + ", sexCD=" + sexCD
				+ ", ageInYearsNum=" + ageInYearsNum + ", languageCD="
				+ languageCD + ", raceCD=" + raceCD + ", maritalStatusCD="
				+ maritalStatusCD + ", religionCD=" + religionCD + ", zipCD="
				+ zipCD + ", stateCityZipPath=" + stateCityZipPath
				+ ", updateDate=" + updateDate + ", downloadDate="
				+ downloadDate + ", importDate=" + importDate
				+ ", sourceSystemCD=" + sourceSystemCD + ", uploadID="
				+ uploadID + ", patientBlob=" + patientBlob + ", incomeCD="
				+ incomeCD + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((ageInYearsNum == null) ? 0 : ageInYearsNum.hashCode());
		result = prime * result
				+ ((birthDate == null) ? 0 : birthDate.hashCode());
		result = prime * result
				+ ((deathDate == null) ? 0 : deathDate.hashCode());
		result = prime * result
				+ ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result
				+ ((importDate == null) ? 0 : importDate.hashCode());
		result = prime * result
				+ ((incomeCD == null) ? 0 : incomeCD.hashCode());
		result = prime * result
				+ ((languageCD == null) ? 0 : languageCD.hashCode());
		result = prime * result
				+ ((maritalStatusCD == null) ? 0 : maritalStatusCD.hashCode());
		result = prime * result
				+ ((patientBlob == null) ? 0 : patientBlob.hashCode());
		result = prime * result
				+ ((patientNum == null) ? 0 : patientNum.hashCode());
		result = prime * result + ((raceCD == null) ? 0 : raceCD.hashCode());
		result = prime * result
				+ ((religionCD == null) ? 0 : religionCD.hashCode());
		result = prime * result + ((sexCD == null) ? 0 : sexCD.hashCode());
		result = prime * result
				+ ((sourceSystemCD == null) ? 0 : sourceSystemCD.hashCode());
		result = prime
				* result
				+ ((stateCityZipPath == null) ? 0 : stateCityZipPath.hashCode());
		result = prime * result
				+ ((updateDate == null) ? 0 : updateDate.hashCode());
		result = prime * result
				+ ((uploadID == null) ? 0 : uploadID.hashCode());
		result = prime * result
				+ ((vitalStatusCD == null) ? 0 : vitalStatusCD.hashCode());
		result = prime * result + ((zipCD == null) ? 0 : zipCD.hashCode());
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
		PatientDimension other = (PatientDimension) obj;
		if (ageInYearsNum == null) {
			if (other.ageInYearsNum != null)
				return false;
		} else if (!ageInYearsNum.equals(other.ageInYearsNum))
			return false;
		if (birthDate == null) {
			if (other.birthDate != null)
				return false;
		} else if (!birthDate.equals(other.birthDate))
			return false;
		if (deathDate == null) {
			if (other.deathDate != null)
				return false;
		} else if (!deathDate.equals(other.deathDate))
			return false;
		if (downloadDate == null) {
			if (other.downloadDate != null)
				return false;
		} else if (!downloadDate.equals(other.downloadDate))
			return false;
		if (importDate == null) {
			if (other.importDate != null)
				return false;
		} else if (!importDate.equals(other.importDate))
			return false;
		if (incomeCD == null) {
			if (other.incomeCD != null)
				return false;
		} else if (!incomeCD.equals(other.incomeCD))
			return false;
		if (languageCD == null) {
			if (other.languageCD != null)
				return false;
		} else if (!languageCD.equals(other.languageCD))
			return false;
		if (maritalStatusCD == null) {
			if (other.maritalStatusCD != null)
				return false;
		} else if (!maritalStatusCD.equals(other.maritalStatusCD))
			return false;
		if (patientBlob == null) {
			if (other.patientBlob != null)
				return false;
		} else if (!patientBlob.equals(other.patientBlob))
			return false;
		if (patientNum == null) {
			if (other.patientNum != null)
				return false;
		} else if (!patientNum.equals(other.patientNum))
			return false;
		if (raceCD == null) {
			if (other.raceCD != null)
				return false;
		} else if (!raceCD.equals(other.raceCD))
			return false;
		if (religionCD == null) {
			if (other.religionCD != null)
				return false;
		} else if (!religionCD.equals(other.religionCD))
			return false;
		if (sexCD == null) {
			if (other.sexCD != null)
				return false;
		} else if (!sexCD.equals(other.sexCD))
			return false;
		if (sourceSystemCD == null) {
			if (other.sourceSystemCD != null)
				return false;
		} else if (!sourceSystemCD.equals(other.sourceSystemCD))
			return false;
		if (stateCityZipPath == null) {
			if (other.stateCityZipPath != null)
				return false;
		} else if (!stateCityZipPath.equals(other.stateCityZipPath))
			return false;
		if (updateDate == null) {
			if (other.updateDate != null)
				return false;
		} else if (!updateDate.equals(other.updateDate))
			return false;
		if (uploadID == null) {
			if (other.uploadID != null)
				return false;
		} else if (!uploadID.equals(other.uploadID))
			return false;
		if (vitalStatusCD == null) {
			if (other.vitalStatusCD != null)
				return false;
		} else if (!vitalStatusCD.equals(other.vitalStatusCD))
			return false;
		if (zipCD == null) {
			if (other.zipCD != null)
				return false;
		} else if (!zipCD.equals(other.zipCD))
			return false;
		return true;
	}

	public static List<PatientDimension> buildFromEntityFile(List<String[]> list) throws Exception {
		List<PatientDimension> patients = new ArrayList<PatientDimension>();
		for(String[] arr: list) {
			if(arr.length > 0 ) {
				patients.add(new PatientDimension(arr));
			}
		}
		return patients;
	}

}
