package etl.data.export.entities.i2b2;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

import org.reflections.ReflectionUtils;

import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;

public class ObservationFact extends Entity{
	
	
	
	public ObservationFact(String str, String encounterNum, String patientNum,
			String conceptCd, String providerId, String startDate,
			String modifierCd, String valtypeCd, String tvalChar,
			String nvalNum, String valueFlagCd, String quantityNum,
			String unitsCd, String endDate, String locationCd,
			String confidenceNum, String updateDate, String downloadDate,
			String importDate, String sourceSystemCd, String uploadId,
			String observationBlob, String instanceNum) throws Exception {
		super(str);
		this.encounterNum = encounterNum;
		this.patientNum = patientNum;
		this.conceptCd = conceptCd;
		this.providerId = providerId;
		this.startDate = startDate;
		this.modifierCd = modifierCd;
		this.valtypeCd = valtypeCd;
		this.tvalChar = tvalChar;
		this.nvalNum = nvalNum;
		this.valueFlagCd = valueFlagCd;
		this.quantityNum = quantityNum;
		this.unitsCd = unitsCd;
		this.endDate = endDate;
		this.locationCd = locationCd;
		this.confidenceNum = confidenceNum;
		this.updateDate = updateDate;
		this.downloadDate = downloadDate;
		this.importDate = importDate;
		this.sourceSystemCd = sourceSystemCd;
		this.uploadId = uploadId;
		this.observationBlob = observationBlob;
		this.instanceNum = instanceNum;
	}

	public ObservationFact(String str) throws Exception {
		super(str);
		this.schema = "I2B2DEMODATA";

		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping, String[] data){
		
		String dataCell = data[new Integer(mapping.getColumnNumber()) - 1]; 				
				
		if(!mapping.getDataLabel().equalsIgnoreCase("OMIT") && dataCell != null && !dataCell.isEmpty() && dataCell.trim().length() > 0 ){
			// currently only handling one encounter
			this.encounterNum = mapping.getVisitIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getVisitIdColumn()) -1];
			this.patientNum = mapping.getPatientIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getPatientIdColumn()) -1];
			
			if(mapping.getDataType().equalsIgnoreCase("n")){
				// numeric nodes are placed all on the same conceptcd;
				this.conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel();
			} else {
				// else they are text
				this.conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel() + ":" + dataCell;
			}
			
			
			this.providerId = mapping.getProviderIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getProviderIdColumn()) -1];
			this.startDate = null;
			this.modifierCd = null;
			this.valtypeCd = mapping.getDataType();
			
			if(mapping.getDataType().equalsIgnoreCase("n")){
				this.tvalChar = "E";
				this.nvalNum = dataCell;
			} else {
				this.tvalChar = dataCell;
				this.nvalNum = null;
			}
			this.valueFlagCd = null;
			this.quantityNum = null;
			this.unitsCd = null;
			this.endDate = null;
			this.locationCd = null;
			this.confidenceNum = null;
			this.updateDate = null;
			this.downloadDate = null;
			this.importDate = null;
			this.sourceSystemCd = this.SOURCESYSTEM_CD;
			this.uploadId = null;
			this.observationBlob = null;
			this.instanceNum = null;
		
			// Concept_cd set to hash
			//this.conceptCd = new Integer(this.hashCode()).toString();
		}
	};

	// Constructor for CsvToI2b2TM
	public ObservationFact(String str, CsvToI2b2TMMapping mapping, String[] data) throws Exception{
		super(str);
		
		String dataCell = data[new Integer(mapping.getColumnNumber()) - 1]; 				
		
		if(!mapping.getDataLabel().equalsIgnoreCase("OMIT") && dataCell != null && !dataCell.isEmpty() && dataCell.trim().length() > 0 ){
			// currently only handling one encounter
			this.encounterNum = mapping.getVisitIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getVisitIdColumn()) -1];
			this.patientNum = mapping.getPatientIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getPatientIdColumn()) -1];
			this.conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel() + ":" + dataCell;
			this.providerId = mapping.getProviderIdColumn().isEmpty() ? "-1" : data[new Integer(mapping.getProviderIdColumn()) -1];
			this.startDate = null;
			this.modifierCd = null;
			this.valtypeCd = mapping.getDataType();
			if(this.valtypeCd.equalsIgnoreCase("n")){
				this.tvalChar = "E";
				this.nvalNum = dataCell;
			} else {
				this.tvalChar = dataCell;
				this.nvalNum = null;
			}
			this.valueFlagCd = null;
			this.quantityNum = null;
			this.unitsCd = null;
			this.endDate = null;
			this.locationCd = null;
			this.confidenceNum = null;
			this.updateDate = null;
			this.downloadDate = null;
			this.importDate = null;
			this.sourceSystemCd = null;
			this.uploadId = null;
			this.observationBlob = null;
			this.instanceNum = null;
		}

	}
	
	private String encounterNum;
	private String patientNum;
	private String conceptCd;
	private String providerId;
	private String startDate;
	private String modifierCd;
	private String valtypeCd; 
	private String tvalChar;
	private String nvalNum;
	private String valueFlagCd;
	private String quantityNum;
	private String unitsCd;
	private String endDate;
	private String locationCd;
	private String confidenceNum;
	private String updateDate;
	private String downloadDate;
	private String importDate;
	private String sourceSystemCd;
	private String uploadId;
	private String observationBlob;
	private String instanceNum;
	
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
		this.conceptCd = conceptCd;
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
		this.tvalChar = tvalChar;
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
	
	@Override
	
	public String toCsv(){
		return makeStringSafe(encounterNum) + "," +
				makeStringSafe(patientNum) + "," +
				makeStringSafe(conceptCd) + "," +
				makeStringSafe(providerId) + "," +
				makeStringSafe(startDate) + "," +
				makeStringSafe(modifierCd) + "," +
				makeStringSafe(valtypeCd) + "," +
				makeStringSafe(tvalChar) + "," +
				makeStringSafe(nvalNum) + "," +
				makeStringSafe(valueFlagCd) + "," +
				makeStringSafe(quantityNum) + "," +
				makeStringSafe(unitsCd) + "," +
				makeStringSafe(endDate) + "," +
				makeStringSafe(locationCd) + "," +
				makeStringSafe(confidenceNum) + "," +
				makeStringSafe(updateDate) + "," +
				makeStringSafe(downloadDate) + "," +
				makeStringSafe(importDate) + "," +
				makeStringSafe(sourceSystemCd) + "," +
				makeStringSafe(uploadId) + "," +
				makeStringSafe(observationBlob) + "," +
				makeStringSafe(instanceNum) + "";
	}


	@Override
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
	
	
}
