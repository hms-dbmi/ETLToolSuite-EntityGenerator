package etl.job.entity.i2b2tm;

import java.util.ArrayList;
import java.util.List;

import com.opencsv.bean.CsvBindByPosition;

public class ObservationFact implements Comparable<ObservationFact>{
	
	@CsvBindByPosition(position = 0)
	private String encounterNum;
	@CsvBindByPosition(position = 1)
	private String patientNum;
	@CsvBindByPosition(position = 2)
	private String conceptCd;
	@CsvBindByPosition(position = 3)
	private String providerId;
	@CsvBindByPosition(position = 4)
	private String startDate;
	@CsvBindByPosition(position = 5)
	private String modifierCd;
	@CsvBindByPosition(position = 6)
	private String valtypeCd; 
	@CsvBindByPosition(position = 7)
	private String tvalChar;
	@CsvBindByPosition(position = 8)
	private String nvalNum;
	@CsvBindByPosition(position = 9)
	private String valueFlagCd;
	@CsvBindByPosition(position = 10)
	private String quantityNum;
	@CsvBindByPosition(position = 11)
	private String unitsCd;
	@CsvBindByPosition(position = 12)
	private String endDate;
	@CsvBindByPosition(position = 13)
	private String locationCd;
	@CsvBindByPosition(position = 14)
	private String confidenceNum;
	@CsvBindByPosition(position = 15)
	private String updateDate;
	@CsvBindByPosition(position = 16)
	private String downloadDate;
	@CsvBindByPosition(position = 17)
	private String importDate;
	@CsvBindByPosition(position = 18)
	private String sourceSystemCd;
	@CsvBindByPosition(position = 19)
	private String uploadId;
	@CsvBindByPosition(position = 20)
	private String observationBlob;
	@CsvBindByPosition(position = 21)
	private String instanceNum;
	
	public ObservationFact() {

	}
	public ObservationFact(String[] arr) throws Exception {
		if(arr.length != 22) {
			throw new Exception("Record missing columns!");
		}
		this.encounterNum = arr[0];
		this.patientNum = arr[1];
		this.conceptCd = arr[2];
		this.providerId = arr[3];
		this.startDate = arr[4];
		this.modifierCd = arr[5];
		this.valtypeCd = arr[6]; 
		this.tvalChar = arr[7];
		this.nvalNum = arr[8];
		this.valueFlagCd = arr[9];
		this.quantityNum = arr[10];
		this.unitsCd = arr[11];
		this.endDate = arr[12];
		this.locationCd = arr[13];
		this.confidenceNum = arr[14];
		this.updateDate = arr[15];
		this.downloadDate = arr[16];
		this.importDate = arr[17];
		this.sourceSystemCd = arr[18];
		this.uploadId = arr[19];
		this.observationBlob = arr[20];
		this.instanceNum = arr[21];
	}	
	public String getEncounterNum() {
		return encounterNum;
	}
	public void setEncounterNum(String encounterNum) {
		this.encounterNum = encounterNum;
	}
	public String getPatientNum() {
		return patientNum;
	}
	public void setPatientNum(String patientNum) {
		this.patientNum = patientNum;
	}
	public String getConceptCd() {
		return conceptCd;
	}
	public void setConceptCd(String conceptCd) {
		this.conceptCd = conceptCd.trim();
	}
	public String getProviderId() {
		return providerId;
	}
	public void setProviderId(String providerId) {
		this.providerId = providerId;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getModifierCd() {
		return modifierCd;
	}
	public void setModifierCd(String modifierCd) {
		this.modifierCd = modifierCd;
	}
	public String getValtypeCd() {
		return valtypeCd;
	}
	public void setValtypeCd(String valtypeCd) {
		this.valtypeCd = valtypeCd;
	}
	public String getTvalChar() {
		return tvalChar;
	}
	public void setTvalChar(String tvalChar) {
		this.tvalChar = tvalChar.trim();
	}
	public String getNvalNum() {
		return nvalNum;
	}
	public void setNvalNum(String nvalNum) {
		this.nvalNum = nvalNum;
	}
	public String getValueFlagCd() {
		return valueFlagCd;
	}
	public void setValueFlagCd(String valueFlagCd) {
		this.valueFlagCd = valueFlagCd;
	}
	public String getQuantityNum() {
		return quantityNum;
	}
	public void setQuantityNum(String quantityNum) {
		this.quantityNum = quantityNum;
	}
	public String getUnitsCd() {
		return unitsCd;
	}
	public void setUnitsCd(String unitsCd) {
		this.unitsCd = unitsCd;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getLocationCd() {
		return locationCd;
	}
	public void setLocationCd(String locationCd) {
		this.locationCd = locationCd;
	}
	public String getConfidenceNum() {
		return confidenceNum;
	}
	public void setConfidenceNum(String confidenceNum) {
		this.confidenceNum = confidenceNum;
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
	public String getSourceSystemCd() {
		return sourceSystemCd;
	}
	public void setSourceSystemCd(String sourceSystemCd) {
		this.sourceSystemCd = sourceSystemCd;
	}
	public String getUploadId() {
		return uploadId;
	}
	public void setUploadId(String uploadId) {
		this.uploadId = uploadId;
	}
	public String getObservationBlob() {
		return observationBlob;
	}
	public void setObservationBlob(String observationBlob) {
		this.observationBlob = observationBlob;
	}
	public String getInstanceNum() {
		return instanceNum;
	}
	public void setInstanceNum(String instanceNum) {
		this.instanceNum = instanceNum;
	}
	@Override
	public String toString() {
		return "ObservationFact [encounterNum=" + encounterNum
				+ ", patientNum=" + patientNum + ", conceptCd=" + conceptCd
				+ ", providerId=" + providerId + ", startDate=" + startDate
				+ ", modifierCd=" + modifierCd + ", valtypeCd=" + valtypeCd
				+ ", tvalChar=" + tvalChar + ", nvalNum=" + nvalNum
				+ ", valueFlagCd=" + valueFlagCd + ", quantityNum="
				+ quantityNum + ", unitsCd=" + unitsCd + ", endDate=" + endDate
				+ ", locationCd=" + locationCd + ", confidenceNum="
				+ confidenceNum + ", updateDate=" + updateDate
				+ ", downloadDate=" + downloadDate + ", importDate="
				+ importDate + ", sourceSystemCd=" + sourceSystemCd
				+ ", uploadId=" + uploadId + ", observationBlob="
				+ observationBlob + ", instanceNum=" + instanceNum + "]";
	}
	
	public boolean isValid() {
		
		return ( this.conceptCd != null && this.patientNum != null ) ? true: false;  
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((conceptCd == null) ? 0 : conceptCd.hashCode());
		result = prime * result
				+ ((confidenceNum == null) ? 0 : confidenceNum.hashCode());
		result = prime * result
				+ ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result
				+ ((encounterNum == null) ? 0 : encounterNum.hashCode());
		result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
		result = prime * result
				+ ((importDate == null) ? 0 : importDate.hashCode());
		result = prime * result
				+ ((instanceNum == null) ? 0 : instanceNum.hashCode());
		result = prime * result
				+ ((locationCd == null) ? 0 : locationCd.hashCode());
		result = prime * result
				+ ((modifierCd == null) ? 0 : modifierCd.hashCode());
		result = prime * result + ((nvalNum == null) ? 0 : nvalNum.hashCode());
		result = prime * result
				+ ((observationBlob == null) ? 0 : observationBlob.hashCode());
		result = prime * result
				+ ((patientNum == null) ? 0 : patientNum.hashCode());
		result = prime * result
				+ ((providerId == null) ? 0 : providerId.hashCode());
		result = prime * result
				+ ((quantityNum == null) ? 0 : quantityNum.hashCode());
		result = prime * result
				+ ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
		result = prime * result
				+ ((startDate == null) ? 0 : startDate.hashCode());
		result = prime * result
				+ ((tvalChar == null) ? 0 : tvalChar.hashCode());
		result = prime * result + ((unitsCd == null) ? 0 : unitsCd.hashCode());
		result = prime * result
				+ ((updateDate == null) ? 0 : updateDate.hashCode());
		result = prime * result
				+ ((uploadId == null) ? 0 : uploadId.hashCode());
		result = prime * result
				+ ((valtypeCd == null) ? 0 : valtypeCd.hashCode());
		result = prime * result
				+ ((valueFlagCd == null) ? 0 : valueFlagCd.hashCode());
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
		ObservationFact other = (ObservationFact) obj;
		if (conceptCd == null) {
			if (other.conceptCd != null)
				return false;
		} else if (!conceptCd.equals(other.conceptCd))
			return false;
		if (confidenceNum == null) {
			if (other.confidenceNum != null)
				return false;
		} else if (!confidenceNum.equals(other.confidenceNum))
			return false;
		if (downloadDate == null) {
			if (other.downloadDate != null)
				return false;
		} else if (!downloadDate.equals(other.downloadDate))
			return false;
		if (encounterNum == null) {
			if (other.encounterNum != null)
				return false;
		} else if (!encounterNum.equals(other.encounterNum))
			return false;
		if (endDate == null) {
			if (other.endDate != null)
				return false;
		} else if (!endDate.equals(other.endDate))
			return false;
		if (importDate == null) {
			if (other.importDate != null)
				return false;
		} else if (!importDate.equals(other.importDate))
			return false;
		if (instanceNum == null) {
			if (other.instanceNum != null)
				return false;
		} else if (!instanceNum.equals(other.instanceNum))
			return false;
		if (locationCd == null) {
			if (other.locationCd != null)
				return false;
		} else if (!locationCd.equals(other.locationCd))
			return false;
		if (modifierCd == null) {
			if (other.modifierCd != null)
				return false;
		} else if (!modifierCd.equals(other.modifierCd))
			return false;
		if (nvalNum == null) {
			if (other.nvalNum != null)
				return false;
		} else if (!nvalNum.equals(other.nvalNum))
			return false;
		if (observationBlob == null) {
			if (other.observationBlob != null)
				return false;
		} else if (!observationBlob.equals(other.observationBlob))
			return false;
		if (patientNum == null) {
			if (other.patientNum != null)
				return false;
		} else if (!patientNum.equals(other.patientNum))
			return false;
		if (providerId == null) {
			if (other.providerId != null)
				return false;
		} else if (!providerId.equals(other.providerId))
			return false;
		if (quantityNum == null) {
			if (other.quantityNum != null)
				return false;
		} else if (!quantityNum.equals(other.quantityNum))
			return false;
		if (sourceSystemCd == null) {
			if (other.sourceSystemCd != null)
				return false;
		} else if (!sourceSystemCd.equals(other.sourceSystemCd))
			return false;
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		if (tvalChar == null) {
			if (other.tvalChar != null)
				return false;
		} else if (!tvalChar.equals(other.tvalChar))
			return false;
		if (unitsCd == null) {
			if (other.unitsCd != null)
				return false;
		} else if (!unitsCd.equals(other.unitsCd))
			return false;
		if (updateDate == null) {
			if (other.updateDate != null)
				return false;
		} else if (!updateDate.equals(other.updateDate))
			return false;
		if (uploadId == null) {
			if (other.uploadId != null)
				return false;
		} else if (!uploadId.equals(other.uploadId))
			return false;
		if (valtypeCd == null) {
			if (other.valtypeCd != null)
				return false;
		} else if (!valtypeCd.equals(other.valtypeCd))
			return false;
		if (valueFlagCd == null) {
			if (other.valueFlagCd != null)
				return false;
		} else if (!valueFlagCd.equals(other.valueFlagCd))
			return false;
		return true;
	}

	@Override
	public int compareTo(ObservationFact o) {
		return 0;
	}

	public static List<ObservationFact> buildFromEntityFile(List<String[]> recs) throws Exception {
		List<ObservationFact> facts = new ArrayList<ObservationFact>();
		for(String[] arr: recs) {
			if(arr.length > 0) {
				facts.add(new ObservationFact(arr));
			}
		}
		return facts;
	}
	
	
}
