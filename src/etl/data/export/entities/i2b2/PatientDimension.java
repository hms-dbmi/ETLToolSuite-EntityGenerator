package etl.data.export.entities.i2b2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.PatientMapping;

public class PatientDimension extends Entity {
	private String patientNum;
	private String vitalStatusCD;
	private String birthDate;
	private String deathDate;
	private String sexCD;
	private String ageInYearsNum;
	private String languageCD;
	private String raceCD;
	private String maritalStatusCD;
	private String religionCD;
	private String zipCD;
	private String stateCityZipPath;
	private String updateDate;
	private String downloadDate;
	private String importDate;
	private String sourceSystemCD;
	private String uploadID;
	private String patientBlob;
	private String incomeCD;
	
	public PatientDimension(String str) throws Exception {
		super(str);
		this.schema = "I2B2DEMODATA";

		// TODO Auto-generated constructor stub
	}
	
	private boolean containsValues(Map<String, List<Object>> valueMap, String fieldKey) {
		if(valueMap.get(fieldKey) == null) {
			return false;
		} else if(valueMap.get(fieldKey).isEmpty()) {
			return false;
		} else if(valueMap.get(fieldKey).get(0) == null) {
			return false;
		} else {
			return true;
		}
		
	}
	
	public PatientDimension(String string, LinkedHashMap record, Map<String, List<Object>> valueMap) throws Exception {
		super(string);
		this.schema = "I2B2DEMODATA";
				
		for(String fieldKey: valueMap.keySet()) {
			
			this.patientNum = fieldKey.equalsIgnoreCase("patientNum") && containsValues(valueMap,fieldKey) ? valueMap.get(fieldKey).get(0).toString() : this.patientNum;
			this.vitalStatusCD = fieldKey.equalsIgnoreCase("vitalStatusCD") && containsValues(valueMap,fieldKey) ? valueMap.get(fieldKey).get(0).toString() : this.vitalStatusCD;
			this.birthDate = fieldKey.equalsIgnoreCase("birthDate")  && containsValues(valueMap,fieldKey) ? valueMap.get(fieldKey).get(0).toString() : this.birthDate;
			this.deathDate = fieldKey.equalsIgnoreCase("deathDate")  && containsValues(valueMap,fieldKey) ? valueMap.get(fieldKey).get(0).toString() : this.deathDate;
			this.sexCD = fieldKey.equalsIgnoreCase("sexCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.sexCD;
			this.ageInYearsNum = fieldKey.equalsIgnoreCase("ageInYearsNum") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.ageInYearsNum;
			this.languageCD = fieldKey.equalsIgnoreCase("languageCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.languageCD;
			this.raceCD = fieldKey.equalsIgnoreCase("raceCD") && containsValues(valueMap,fieldKey) && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.raceCD;
			this.maritalStatusCD = fieldKey.equalsIgnoreCase("maritalStatusCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.maritalStatusCD;
			this.religionCD = fieldKey.equalsIgnoreCase("religionCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.religionCD;
			this.zipCD = fieldKey.equalsIgnoreCase("zipCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.zipCD;
			this.stateCityZipPath = fieldKey.equalsIgnoreCase("stateCityZipPath") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.stateCityZipPath;
			this.updateDate = fieldKey.equalsIgnoreCase("updateDate") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.updateDate;
			this.downloadDate = fieldKey.equalsIgnoreCase("downloadDate") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.downloadDate;
			this.importDate = fieldKey.equalsIgnoreCase("importDate") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.importDate;
			this.sourceSystemCD = fieldKey.equalsIgnoreCase("sourceSystemCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.sourceSystemCD;
			this.uploadID = fieldKey.equalsIgnoreCase("uploadID") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.uploadID;
			this.patientBlob = fieldKey.equalsIgnoreCase("patientBlob") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.patientBlob;
			this.incomeCD = fieldKey.equalsIgnoreCase("incomeCD") && containsValues(valueMap,fieldKey)  ? valueMap.get(fieldKey).get(0).toString() : this.incomeCD;

		}
	}

	public PatientDimension(String string, Map<String, String> map) throws Exception {
		super(string);
		this.patientNum = map.get("patientNum");
		this.sexCD = map.get("sexCD");
		this.ageInYearsNum = map.get("ageInYearsNum");
		this.raceCD = map.get("raceCD");
	}

	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping,
			String[] data) {
		this.patientNum = mapping.getPatientIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getPatientIdColumn()) -1];
		this.vitalStatusCD = null;
		this.birthDate = null;
		this.deathDate = null;
		this.sexCD = null;
		this.ageInYearsNum = null;
		this.languageCD = null;
		this.raceCD = null;
		this.maritalStatusCD = null;
		this.religionCD = null;
		this.zipCD = null;
		this.stateCityZipPath = null;
		this.updateDate = null;
		this.downloadDate = null;
		this.importDate = null;
		this.sourceSystemCD = this.SOURCESYSTEM_CD;
		this.uploadID = null;
		this.patientBlob = null;
		this.incomeCD = null;
	}

	@Override
	public boolean isValid() {
		return (this.patientNum != null && !this.patientNum.isEmpty()) ? true: false;
	}

	@Override
	public String toCsv() {
		return makeStringSafe(patientNum) + "," + makeStringSafe(vitalStatusCD) + "," 
				+ makeStringSafe(birthDate) + "," + makeStringSafe(deathDate) + "," + makeStringSafe(sexCD) + "," 
				+ makeStringSafe(ageInYearsNum) + "," + makeStringSafe(languageCD) + "," 
				+ makeStringSafe(raceCD) + "," + makeStringSafe(maritalStatusCD) + "," + makeStringSafe(religionCD) + "," 
				+ makeStringSafe(zipCD) + "," + makeStringSafe(stateCityZipPath) + "," 
				+ makeStringSafe(updateDate) + "," + makeStringSafe(downloadDate) + "," + makeStringSafe(importDate) + "," 
				+ makeStringSafe(sourceSystemCD) + "," + makeStringSafe(uploadID) + "," 
				+ makeStringSafe(patientBlob) + "," + makeStringSafe(incomeCD);
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

	private List<PatientDimension> generatePatientDimension(List records, PatientMapping patientMapping) {
		
		List<PatientDimension> patients = new ArrayList<PatientDimension>();
		
		if(records != null) {
			for(Object o: records) {
				// downcast object to LinkedHashMap
				LinkedHashMap<String, Object> m = (LinkedHashMap<String, Object>) o;
				
				
				//patients.addAll(PatientMapping.processMapping(m, patientMapping));
			}
			
			
		}
		
		return patients;
	}
}
