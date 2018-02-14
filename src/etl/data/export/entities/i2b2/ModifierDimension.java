package etl.data.export.entities.i2b2;

import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;

public class ModifierDimension extends Entity {

	private String modifierPath;
	private String modifierCd;
	private String nameChar;
	private String modifierBlob;
	private String updateDate;
	private String downloadDate;
	private String importDate;
	private String sourceSystemCd;
	private String uploadId;
	
	
	public ModifierDimension(String str) throws Exception {
		super(str);
		this.schema = "I2B2DEMODATA";
		// TODO Auto-generated constructor stub
	}

	
	
	public String getModifierPath() {
		return modifierPath;
	}



	public void setModifierPath(String modifierPath) {
		this.modifierPath = modifierPath;
	}



	public String getModifierCd() {
		return modifierCd;
	}



	public void setModifierCd(String modifierCd) {
		this.modifierCd = modifierCd;
	}



	public String getNameChar() {
		return nameChar;
	}



	public void setNameChar(String nameChar) {
		this.nameChar = nameChar;
	}



	public String getModifierBlob() {
		return modifierBlob;
	}



	public void setModifierBlob(String modifierBlob) {
		this.modifierBlob = modifierBlob;
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



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result
				+ ((importDate == null) ? 0 : importDate.hashCode());
		result = prime * result
				+ ((modifierBlob == null) ? 0 : modifierBlob.hashCode());
		result = prime * result
				+ ((modifierCd == null) ? 0 : modifierCd.hashCode());
		result = prime * result
				+ ((modifierPath == null) ? 0 : modifierPath.hashCode());
		result = prime * result
				+ ((nameChar == null) ? 0 : nameChar.hashCode());
		result = prime * result
				+ ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
		result = prime * result
				+ ((updateDate == null) ? 0 : updateDate.hashCode());
		result = prime * result
				+ ((uploadId == null) ? 0 : uploadId.hashCode());
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
		ModifierDimension other = (ModifierDimension) obj;
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
		if (modifierBlob == null) {
			if (other.modifierBlob != null)
				return false;
		} else if (!modifierBlob.equals(other.modifierBlob))
			return false;
		if (modifierCd == null) {
			if (other.modifierCd != null)
				return false;
		} else if (!modifierCd.equals(other.modifierCd))
			return false;
		if (modifierPath == null) {
			if (other.modifierPath != null)
				return false;
		} else if (!modifierPath.equals(other.modifierPath))
			return false;
		if (nameChar == null) {
			if (other.nameChar != null)
				return false;
		} else if (!nameChar.equals(other.nameChar))
			return false;
		if (sourceSystemCd == null) {
			if (other.sourceSystemCd != null)
				return false;
		} else if (!sourceSystemCd.equals(other.sourceSystemCd))
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
		return true;
	}



	@Override
	public String toString() {
		return "ModifierDimension [modifierPath=" + modifierPath
				+ ", modifierCd=" + modifierCd + ", nameChar=" + nameChar
				+ ", modifierBlob=" + modifierBlob + ", updateDate="
				+ updateDate + ", downloadDate=" + downloadDate
				+ ", importDate=" + importDate + ", sourceSystemCd="
				+ sourceSystemCd + ", uploadId=" + uploadId + "]";
	}



	@Override
	public String toCsv() {
		return makeStringSafe(modifierPath) + "," +
				makeStringSafe(modifierCd) + "," + 
				makeStringSafe(nameChar) + "," + 
				makeStringSafe(modifierBlob) + "," + 
				makeStringSafe(updateDate) + "," +  
				makeStringSafe(downloadDate) + "," +  
				makeStringSafe(importDate) + "," +  
				makeStringSafe(sourceSystemCd) + "," +  
				makeStringSafe(uploadId);
	}



	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping,
			String[] data) {
		// TODO Auto-generated method stub
		
	}



	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}

}
