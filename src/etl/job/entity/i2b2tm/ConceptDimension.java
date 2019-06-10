package etl.job.entity.i2b2tm;

import com.opencsv.bean.CsvBindByPosition;

public class ConceptDimension {
	@CsvBindByPosition(position = 0)
	private String conceptCd;
	@CsvBindByPosition(position = 1)
	private String conceptPath;
	@CsvBindByPosition(position = 2)
	private String downloadDate;
	@CsvBindByPosition(position = 3)
	private String nameChar;
	@CsvBindByPosition(position = 4)
	private String conceptBlob;
	@CsvBindByPosition(position = 5)
	private String updateDate;
	@CsvBindByPosition(position = 6)
	private String importDate;
	@CsvBindByPosition(position = 7)
	private String sourceSystemCd;
	@CsvBindByPosition(position = 8)
	private String uploadId;
	@CsvBindByPosition(position = 9)
	private String tableName;
	
	// sequences
	
	public ConceptDimension() {

	}

	public boolean isValid() {
		return  this.conceptCd != null ? true: false;
	}
	public String getConceptCd() {
		return conceptCd;
	}

	public void setConceptCd(String conceptCd) {
		conceptCd = conceptCd.trim();

		this.conceptCd = conceptCd;
	}

	public String getConceptPath() {
		return conceptPath;
	}

	public void setConceptPath(String conceptPath) {
		this.conceptPath = conceptPath;
	}

	public String getNameChar() {
		return nameChar;
	}

	public void setNameChar(String nameChar) {
		this.nameChar = nameChar;
	}

	public String getConceptBlob() {
		return conceptBlob;
	}

	public void setConceptBlob(String conceptBlob) {
		this.conceptBlob = conceptBlob;
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

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String toString() {
		return "ConceptDimension [conceptCd=" + conceptCd + ", conceptPath="
				+ conceptPath + ", nameChar=" + nameChar + ", conceptBlob="
				+ conceptBlob + ", updateDate=" + updateDate
				+ ", downloadDate=" + downloadDate + ", importDate="
				+ importDate + ", sourceSystemCd=" + sourceSystemCd
				+ ", uploadId=" + uploadId + ", tableName=" + tableName + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((conceptCd == null) ? 0 : conceptCd.hashCode());
		result = prime * result + ((conceptPath == null) ? 0 : conceptPath.hashCode());
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
		ConceptDimension other = (ConceptDimension) obj;
		if (conceptCd == null) {
			if (other.conceptCd != null)
				return false;
		} else if (!conceptCd.equals(other.conceptCd))
			return false;
		if (conceptPath == null) {
			if (other.conceptPath != null)
				return false;
		} else if (!conceptPath.equals(other.conceptPath))
			return false;
		return true;
	}



}
