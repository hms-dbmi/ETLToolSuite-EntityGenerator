package etl.job.entity.i2b2tm;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.CsvBindByPosition;

public class I2B2 implements Cloneable{
	@CsvBindByPosition(position = 0)
	private String cHlevel;
	@CsvBindByPosition(position = 1)
	private String cFullName;
	@CsvBindByPosition(position = 2)
	private String cName;
	@CsvBindByPosition(position = 3)
	private String cSynonymCd = "N";
	@CsvBindByPosition(position = 4)
	private String cVisualAttributes;
	@CsvBindByPosition(position = 5)
	private String cTotalNum;
	@CsvBindByPosition(position = 6)
	private String cBaseCode;
	@CsvBindByPosition(position = 7)
	private String cMetaDataXML;
	@CsvBindByPosition(position = 8)
	private String cFactTableColumn;
	@CsvBindByPosition(position = 9)
	private String cTableName;
	@CsvBindByPosition(position = 10)
	private String cColumnName;
	@CsvBindByPosition(position = 11)
	private String cColumnDataType = "T";
	@CsvBindByPosition(position = 12)
	private String cOperator;
	@CsvBindByPosition(position = 13)
	private String cDimCode;
	@CsvBindByPosition(position = 14)
	private String cComment;
	@CsvBindByPosition(position = 15)
	private String cToolTip;
	@CsvBindByPosition(position = 16)
	private String updateDate;
	@CsvBindByPosition(position = 17)
	private String downloadDate;
	@CsvBindByPosition(position = 18)
	private String importDate;
	@CsvBindByPosition(position = 19)
	private String sourceSystemCd;
	@CsvBindByPosition(position = 20)
	private String valueTypeCd;
	@CsvBindByPosition(position = 21)
	private String i2b2Id;
	@CsvBindByPosition(position = 22)
	private String mAppliedPath;
	@CsvBindByPosition(position = 23)
	private String mExclusionCd;
	@CsvBindByPosition(position = 24)
	private String cPath;
	@CsvBindByPosition(position = 25)
	private String cSymbol;
	
		
	@Override
	public Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	public I2B2() {

	}

	public I2B2(String[] record) {
		cHlevel = record[0];
		cFullName = record[1];
		cName = record[2];
		cSynonymCd = record[3];
		cVisualAttributes = record[4];
		cTotalNum = record[5];
		cBaseCode = record[6];
		cMetaDataXML = record[7];
		cFactTableColumn = record[8];
		cTableName = record[9];
		cColumnName = record[10];
		cColumnDataType = record[11];
		cOperator = record[12];
		cDimCode = record[13];
		cComment = record[14];
		cToolTip = record[15];
		updateDate = record[16];
		downloadDate = record[17];
		importDate = record[18];
		sourceSystemCd = record[19];
		valueTypeCd = record[20];
		i2b2Id = record[21];
		mAppliedPath = record[22];
		mExclusionCd = record[23];
		cPath = record[24];
		cSymbol = record[25];	
	}

	public I2B2(I2B2 node) {
		this.cHlevel = node.cHlevel;
		this.cFullName = node.cFullName;
		this.cName = node.cName;
		this.cSynonymCd = node.cSynonymCd;
		this.cVisualAttributes = node.cVisualAttributes;
		this.cTotalNum = node.cTotalNum;
		this.cBaseCode = node.cBaseCode;
		this.cMetaDataXML = node.cMetaDataXML;
		this.cFactTableColumn = node.cFactTableColumn;
		this.cTableName = node.cTableName;
		this.cColumnName = node.cColumnName;
		this.cColumnDataType = node.cColumnDataType;
		this.cOperator = node.cOperator;
		this.cDimCode = node.cDimCode;
		this.cComment = node.cComment;
		this.cToolTip = node.cToolTip;
		this.updateDate = node.updateDate;
		this.downloadDate = node.downloadDate;
		this.importDate = node.importDate;
		this.sourceSystemCd = node.sourceSystemCd;
		this.valueTypeCd = node.valueTypeCd;
		this.i2b2Id = node.i2b2Id;
		this.mAppliedPath = node.mAppliedPath;
		this.mExclusionCd = node.mExclusionCd;
		this.cPath = node.cPath;
		this.cSymbol = node.cSymbol;
	}

	public boolean isValid() {
		return (this.cFullName != null && this.cName != null) ? true: false;
	}
	
	public String getcHlevel() {
		return cHlevel;
	}

	public void setcHlevel(String cHlevel) {
		this.cHlevel = cHlevel;
	}

	public String getcFullName() {
		return cFullName;
	}

	public void setcFullName(String cFullName) {
		this.cFullName = cFullName;
	}

	public String getcName() {
		return cName;
	}

	public void setcName(String cName) {
		this.cName = cName;
	}

	public String getcSynonymCd() {
		return cSynonymCd;
	}

	public void setcSynonymCd(String cSynonymCd) {
		this.cSynonymCd = cSynonymCd;
	}

	public String getcVisualAttributes() {
		return cVisualAttributes;
	}

	public void setcVisualAttributes(String cVisualAttributes) {
		this.cVisualAttributes = cVisualAttributes;
	}

	public String getcTotalNum() {
		return cTotalNum;
	}

	public void setcTotalNum(String cTotalNum) {
		this.cTotalNum = cTotalNum;
	}

	public String getcBaseCode() {
		return cBaseCode;
	}

	public void setcBaseCode(String cBaseCode) {
		this.cBaseCode = cBaseCode;
	}

	public String getcMetaDataXML() {
		return cMetaDataXML;
	}

	public void setcMetaDataXML(String cMetaDataXML) {
		this.cMetaDataXML = cMetaDataXML;
	}

	public String getcFactTableColumn() {
		return cFactTableColumn;
	}

	public void setcFactTableColumn(String cFactTableColumn) {
		this.cFactTableColumn = cFactTableColumn;
	}

	public String getcTableName() {
		return cTableName;
	}

	public void setcTableName(String cTableName) {
		this.cTableName = cTableName;
	}

	public String getcColumnName() {
		return cColumnName;
	}

	public void setcColumnName(String cColumnName) {
		this.cColumnName = cColumnName;
	}

	public String getcColumnDataType() {
		return cColumnDataType;
	}

	public void setcColumnDataType(String cColumnDataType) {
		this.cColumnDataType = cColumnDataType;
	}

	public String getcOperator() {
		return cOperator;
	}

	public void setcOperator(String cOperator) {
		this.cOperator = cOperator;
	}

	public String getcDimCode() {
		return cDimCode;
	}

	public void setcDimCode(String cDimCode) {
		this.cDimCode = cDimCode;
	}

	public String getcComment() {
		return cComment;
	}

	public void setcComment(String cComment) {
		this.cComment = cComment;
	}

	public String getcToolTip() {
		return cToolTip;
	}

	public void setcToolTip(String cToolTip) {
		this.cToolTip = cToolTip;
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

	public String getValueTypeCd() {
		return valueTypeCd;
	}

	public void setValueTypeCd(String valueTypeCd) {
		this.valueTypeCd = valueTypeCd;
	}

	public String getI2b2Id() {
		return i2b2Id;
	}

	public void setI2b2Id(String i2b2Id) {
		this.i2b2Id = i2b2Id;
	}

	public String getmAppliedPath() {
		return mAppliedPath;
	}

	public void setmAppliedPath(String mAppliedPath) {
		this.mAppliedPath = mAppliedPath;
	}

	public String getmExclusionCd() {
		return mExclusionCd;
	}

	public void setmExclusionCd(String mExclusionCd) {
		this.mExclusionCd = mExclusionCd;
	}

	public String getcPath() {
		return cPath;
	}

	public void setcPath(String cPath) {
		this.cPath = cPath;
	}

	public String getcSymbol() {
		return cSymbol;
	}

	public void setcSymbol(String cSymbol) {
		this.cSymbol = cSymbol;
	}

	public String[] toStringArray() {
		
		String[] array = new String[] {
				cHlevel,cFullName,cName,cSynonymCd,cVisualAttributes,
				cTotalNum,cBaseCode,cMetaDataXML,cFactTableColumn,
				cTableName,cColumnName,cColumnDataType,cOperator,
				cDimCode,cComment,cToolTip,updateDate,downloadDate,
				importDate,sourceSystemCd,valueTypeCd,i2b2Id,
				mAppliedPath,mExclusionCd,cPath,cSymbol
		};
		
		return array;
	}
	
	@Override
	public String toString() {
		return "I2B2 [cHlevel=" + cHlevel + ", cFullName=" + cFullName
				+ ", cName=" + cName + ", cSynonymCd=" + cSynonymCd
				+ ", cVisualAttributes=" + cVisualAttributes + ", cTotalNum="
				+ cTotalNum + ", cBaseCode=" + cBaseCode + ", cMetaDataXML="
				+ cMetaDataXML + ", cFactTableColumn=" + cFactTableColumn
				+ ", cTableName=" + cTableName + ", cColumnName=" + cColumnName
				+ ", cColumnDataType=" + cColumnDataType + ", cOperator="
				+ cOperator + ", cDimCode=" + cDimCode + ", cComment="
				+ cComment + ", cToolTip=" + cToolTip + ", updateDate="
				+ updateDate + ", downloadDate=" + downloadDate
				+ ", importDate=" + importDate + ", sourceSystemCd="
				+ sourceSystemCd + ", valueTypeCd=" + valueTypeCd + ", i2b2Id="
				+ i2b2Id + ", mAppliedPath=" + mAppliedPath + ", mExclusionCd="
				+ mExclusionCd + ", cPath=" + cPath + ", cSymbol=" + cSymbol
				+ "]";
	}
	/**
	 * Fill in tree by taking all clevel where the most diversity occurs base nodes then recursively back fill them
	 *  
	 * @param nodes
	 * @throws Exception
	 */

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cDimCode == null) ? 0 : cDimCode.hashCode());
		result = prime * result + ((cFullName == null) ? 0 : cFullName.hashCode());
		result = prime * result + ((cHlevel == null) ? 0 : cHlevel.hashCode());
		result = prime * result + ((cName == null) ? 0 : cName.hashCode());
		result = prime * result + ((mAppliedPath == null) ? 0 : mAppliedPath.hashCode());
		result = prime * result + ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
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
		I2B2 other = (I2B2) obj;
		if (cDimCode == null) {
			if (other.cDimCode != null)
				return false;
		} else if (!cDimCode.equals(other.cDimCode))
			return false;
		if (cFullName == null) {
			if (other.cFullName != null)
				return false;
		} else if (!cFullName.equals(other.cFullName))
			return false;
		if (cHlevel == null) {
			if (other.cHlevel != null)
				return false;
		} else if (!cHlevel.equals(other.cHlevel))
			return false;
		if (cName == null) {
			if (other.cName != null)
				return false;
		} else if (!cName.equals(other.cName))
			return false;
		if (mAppliedPath == null) {
			if (other.mAppliedPath != null)
				return false;
		} else if (!mAppliedPath.equals(other.mAppliedPath))
			return false;
		if (sourceSystemCd == null) {
			if (other.sourceSystemCd != null)
				return false;
		} else if (!sourceSystemCd.equals(other.sourceSystemCd))
			return false;
		return true;
	}


}
