package etl.data.export.entities.i2b2;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import etl.data.export.entities.Entity;
import etl.data.sequence.Sequence;
import etl.mapping.CsvToI2b2TMMapping;

public class ConceptDimension extends Entity{
	private String conceptCd;
	private String conceptPath;
	private String downloadDate;
	private String nameChar;
	private String conceptBlob;
	private String updateDate;
	private String importDate;
	private String sourceSystemCd;
	private String uploadId;
	private String tableName;
	
	// sequences
	public static Sequence conceptCdSeq;
	
	public ConceptDimension(String str) throws Exception {
		super(str);
		this.schema = "I2B2DEMODATA";
		// TODO Auto-generated constructor stub
	}

	
	public ConceptDimension(String str, String conceptCd, String conceptPath,
			String nameChar, String conceptBlob, String updateDate,
			String downloadDate, String importDate, String sourceSystemCd,
			String uploadId, String tableName) throws Exception {
		super(str);
		this.conceptCd = conceptCd;
		this.conceptPath = conceptPath;
		this.nameChar = nameChar;
		this.conceptBlob = conceptBlob;
		this.updateDate = updateDate;
		this.downloadDate = downloadDate;
		this.importDate = importDate;
		this.sourceSystemCd = sourceSystemCd;
		this.uploadId = uploadId;
		this.tableName = tableName;
	}


	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping,
			String[] data) {
		
		String dataCell = data[new Integer(mapping.getColumnNumber()) - 1]; 				

		if(!mapping.getDataLabel().equalsIgnoreCase("OMIT") && dataCell != null && !dataCell.isEmpty() && dataCell.trim().length() > 0 ){

			List<String> path;

			if(mapping.getDataType().equalsIgnoreCase("n")){
				
				path = Arrays.asList(this.ROOT_NODE, mapping.getCategoryCode(), mapping.getDataLabel());
			
			} else {
				
				path = Arrays.asList(this.ROOT_NODE, mapping.getCategoryCode(), mapping.getDataLabel(), dataCell);
			
			}
			
			if(mapping.getDataType().equalsIgnoreCase("n")){
				// numeric nodes are placed all on the same conceptcd;
				this.conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel();
			} else {
				// else they are text
				this.conceptCd = mapping.getCategoryCode() + ":" + mapping.getDataLabel() + ":" + dataCell;
			}
			
			this.conceptPath = buildConceptPath(path);

			if(mapping.getDataType().equalsIgnoreCase("n")){
				this.nameChar = mapping.getDataLabel();;
			} else {
				this.nameChar = dataCell;;
			}
			this.conceptBlob = null;
			this.updateDate = null;
			this.downloadDate = null;
			this.importDate = null;
			this.sourceSystemCd = this.SOURCESYSTEM_CD;
			this.uploadId = null;
			this.tableName = null;
		}
	}

	@Override
	public boolean isValid() {
		return  this.conceptCd != null ? true: false;
	}
	public String getConceptCd() {
		return conceptCd;
	}

	public void setConceptCd(String conceptCd) {
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
	public String toCsv() {
		return 	makeStringSafe(conceptCd) + "," + 
				makeStringSafe(conceptPath) + "," +			
				makeStringSafe(nameChar) + "," +
				makeStringSafe(conceptBlob) + "," +
				makeStringSafe(updateDate) + "," +
				makeStringSafe(downloadDate) + "," +
				makeStringSafe(importDate) + "," +
				makeStringSafe(sourceSystemCd) + "," +
				makeStringSafe(uploadId) + "," +
				makeStringSafe(tableName);	
		
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((conceptBlob == null) ? 0 : conceptBlob.hashCode());
		result = prime * result
				+ ((conceptCd == null) ? 0 : conceptCd.hashCode());
		result = prime * result
				+ ((conceptPath == null) ? 0 : conceptPath.hashCode());
		result = prime * result
				+ ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result
				+ ((importDate == null) ? 0 : importDate.hashCode());
		result = prime * result
				+ ((nameChar == null) ? 0 : nameChar.hashCode());
		result = prime * result
				+ ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
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
		ConceptDimension other = (ConceptDimension) obj;
		if (conceptBlob == null) {
			if (other.conceptBlob != null)
				return false;
		} else if (!conceptBlob.equals(other.conceptBlob))
			return false;
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
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
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

}
