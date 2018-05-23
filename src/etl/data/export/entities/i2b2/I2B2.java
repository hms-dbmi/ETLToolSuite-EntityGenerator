package etl.data.export.entities.i2b2;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;

public class I2B2 extends Entity {
	private String cHlevel;
	private String cFullName;
	private String cName;
	private String cSynonymCd = "N";
	private String cVisualAttributes;
	private String cTotalNum;
	private String cBaseCode;
	private String cMetaDataXML;
	private String cFactTableColumn;
	private String cTableName;
	private String cColumnName;
	private String cColumnDataType = "T";
	private String cOperator;
	private String cDimCode;
	private String cComment;
	private String cToolTip;
	private String updateDate;
	private String downloadDate;
	private String importDate;
	private String sourceSystemCd;
	private String valueTypeCd;
	private String i2b2Id;
	private String mAppliedPath;
	private String mExclusionCd;
	private String cPath;
	private String cSymbol;
	
		
	public I2B2(String str) throws Exception {
		super(str);
		this.schema = "I2B2METADATA";
		
		// TODO Auto-generated constructor stub
	}

	public I2B2(String str, String cHlevel, String cFullName, String cName,
			String cSynonymCd, String cVisualAttributes, String cTotalNum,
			String cBaseCode, String cMetaDataXML, String cFactTableColumn,
			String cTableName, String cColumnName, String cColumnDataType,
			String cOperator, String cDimCode, String cComment,
			String cToolTip, String updateDate, String downloadDate,
			String importDate, String sourceSystemCd, String valueTypeCd,
			String i2b2Id, String mAppliedPath, String mExclusionCd,
			String cPath, String cSymbol)
			throws Exception {
		super(str);
		this.cHlevel = cHlevel;
		this.cFullName = cFullName;
		this.cName = cName;
		this.cSynonymCd = cSynonymCd;
		this.cVisualAttributes = cVisualAttributes;
		this.cTotalNum = cTotalNum;
		this.cBaseCode = cBaseCode;
		this.cMetaDataXML = cMetaDataXML;
		this.cFactTableColumn = cFactTableColumn;
		this.cTableName = cTableName;
		this.cColumnName = cColumnName;
		this.cColumnDataType = cColumnDataType;
		this.cOperator = cOperator;
		this.cDimCode = cDimCode;
		this.cComment = cComment;
		this.cToolTip = cToolTip;
		this.updateDate = updateDate;
		this.downloadDate = downloadDate;
		this.importDate = importDate;
		this.sourceSystemCd = sourceSystemCd;
		this.valueTypeCd = valueTypeCd;
		this.i2b2Id = i2b2Id;
		this.mAppliedPath = mAppliedPath;
		this.mExclusionCd = mExclusionCd;
		this.cPath = cPath;
		this.cSymbol = cSymbol;
	}



	@Deprecated
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
				this.cHlevel = calculateHlevel(buildConceptPath(path)).toString();

				this.cFullName = buildConceptPath(path);
				this.cName = mapping.getDataLabel();
				this.cVisualAttributes = "LA";
				this.cMetaDataXML = "<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>";
				this.cFactTableColumn = "CONCEPT_CD";
				this.cTableName = "CONCEPT_DIMENSION";
				this.cColumnName = "CONCEPT_PATH";
				this.cOperator = "LIKE";
				this.cDimCode = buildConceptPath(path);
				this.cToolTip = buildConceptPath(path);
				this.mAppliedPath = null;
				this.mExclusionCd = null;
				
			} else {
				
				this.cHlevel = calculateHlevel(buildConceptPath(path)).toString();
				this.cFullName = buildConceptPath(path);
				this.cName = dataCell;
				this.cVisualAttributes = "LA";
				this.cMetaDataXML = null;
				this.cFactTableColumn = "CONCEPT_CD";
				this.cTableName = "CONCEPT_DIMENSION";
				this.cColumnName = "CONCEPT_PATH";
				this.cOperator = "LIKE";
				this.cDimCode = buildConceptPath(path);
				this.cToolTip = buildConceptPath(path);
				this.mAppliedPath = null;
				this.mExclusionCd = null;
				
			}

			this.cColumnDataType = "T";
			this.cSynonymCd = null;
			this.cTotalNum = null;
			this.cBaseCode = null;
			this.cComment = null;
			this.updateDate = null;
			this.downloadDate = null;
			this.importDate = null;
			this.sourceSystemCd = this.SOURCESYSTEM_CD;
			this.valueTypeCd = "T";
			this.i2b2Id = null;
			this.cPath = null;
			this.cSymbol = null;
			
		}
	}

	@Override
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

	public static Set<Entity> fillTree(Collection<? extends I2B2> nodes) throws Exception{
		
		Set<Entity> set = new HashSet<Entity>();
		
		for(I2B2 node: nodes){
			
			Integer x = StringUtils.countMatches(node.getcFullName(),"\\") - 1;
			
			while(x > 1){
				
				I2B2 i2b2 = (I2B2) node.clone();
				
				i2b2.setcFullName(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), "\\", x) + 1 ));
				
				i2b2.setcDimCode(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), "\\", x) + 1 ));
				
				i2b2.setcToolTip(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), "\\", x) + 1 ));
				
				i2b2.setcHlevel(new Integer(x - 2).toString());
				i2b2.setcBaseCode(null);
				i2b2.setcVisualAttributes("FA");
				
				i2b2.setcMetaDataXML("");
				
				String[] fullNodes = i2b2.getcFullName().split("\\\\");
				
				i2b2.setcName(fullNodes[fullNodes.length - 1]);
				
				set.add(i2b2);
				
				x--;
				if(i2b2.getcHlevel().equals("0") && i2b2.getcTableName().equalsIgnoreCase("concept_dimension")) {
					
					TableAccess tableAccess = new TableAccess("TableAccess", i2b2);
					
					set.add(tableAccess);
				}
			}
		}
		
		return set;
		
	}

	@Override
	public String toCsv() {
		return makeStringSafe(cHlevel) + "," +
				makeStringSafe(cFullName) + "," + 
				makeStringSafe(cName) + "," + 
				makeStringSafe(cSynonymCd) + "," + 
				makeStringSafe(cVisualAttributes) + "," +  
				makeStringSafe(cTotalNum) + "," +  
				makeStringSafe(cBaseCode) + "," +  
				makeStringSafe(cMetaDataXML) + "," +  
				makeStringSafe(cFactTableColumn) + "," + 
				makeStringSafe(cTableName) + "," +  
				makeStringSafe(cColumnName)	+ "," + 
				makeStringSafe(cColumnDataType) + "," + 
				makeStringSafe(cOperator) + "," +  
				makeStringSafe(cDimCode) + "," +
				makeStringSafe(cComment) + "," +  
				makeStringSafe(cToolTip) + "," +  
				makeStringSafe(updateDate) + "," + 
				makeStringSafe(downloadDate) + "," + 
				makeStringSafe(importDate) +  "," + 
				makeStringSafe(sourceSystemCd) +  "," + 
				makeStringSafe(valueTypeCd) + "," +  
				makeStringSafe(i2b2Id) + "," +  
				makeStringSafe(mAppliedPath) + "," +  
				makeStringSafe(mExclusionCd) + "," +  
				makeStringSafe(cPath) + "," +  
				makeStringSafe(cSymbol);
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((cBaseCode == null) ? 0 : cBaseCode.hashCode());
		result = prime * result
				+ ((cColumnDataType == null) ? 0 : cColumnDataType.hashCode());
		result = prime * result
				+ ((cColumnName == null) ? 0 : cColumnName.hashCode());
		result = prime * result
				+ ((cComment == null) ? 0 : cComment.hashCode());
		result = prime * result
				+ ((cDimCode == null) ? 0 : cDimCode.hashCode());
		result = prime
				* result
				+ ((cFactTableColumn == null) ? 0 : cFactTableColumn.hashCode());
		result = prime * result
				+ ((cFullName == null) ? 0 : cFullName.hashCode());
		result = prime * result + ((cHlevel == null) ? 0 : cHlevel.hashCode());
		result = prime * result
				+ ((cMetaDataXML == null) ? 0 : cMetaDataXML.hashCode());
		result = prime * result + ((cName == null) ? 0 : cName.hashCode());
		result = prime * result
				+ ((cOperator == null) ? 0 : cOperator.hashCode());
		result = prime * result + ((cPath == null) ? 0 : cPath.hashCode());
		result = prime * result + ((cSymbol == null) ? 0 : cSymbol.hashCode());
		result = prime * result
				+ ((cSynonymCd == null) ? 0 : cSynonymCd.hashCode());
		result = prime * result
				+ ((cTableName == null) ? 0 : cTableName.hashCode());
		result = prime * result
				+ ((cToolTip == null) ? 0 : cToolTip.hashCode());
		result = prime * result
				+ ((cTotalNum == null) ? 0 : cTotalNum.hashCode());
		result = prime
				* result
				+ ((cVisualAttributes == null) ? 0 : cVisualAttributes
						.hashCode());
		result = prime * result
				+ ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result + ((i2b2Id == null) ? 0 : i2b2Id.hashCode());
		result = prime * result
				+ ((importDate == null) ? 0 : importDate.hashCode());
		result = prime * result
				+ ((mAppliedPath == null) ? 0 : mAppliedPath.hashCode());
		result = prime * result
				+ ((mExclusionCd == null) ? 0 : mExclusionCd.hashCode());
		result = prime * result
				+ ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
		result = prime * result
				+ ((updateDate == null) ? 0 : updateDate.hashCode());
		result = prime * result
				+ ((valueTypeCd == null) ? 0 : valueTypeCd.hashCode());
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
		if (cBaseCode == null) {
			if (other.cBaseCode != null)
				return false;
		} else if (!cBaseCode.equals(other.cBaseCode))
			return false;
		if (cColumnDataType == null) {
			if (other.cColumnDataType != null)
				return false;
		} else if (!cColumnDataType.equals(other.cColumnDataType))
			return false;
		if (cColumnName == null) {
			if (other.cColumnName != null)
				return false;
		} else if (!cColumnName.equals(other.cColumnName))
			return false;
		if (cComment == null) {
			if (other.cComment != null)
				return false;
		} else if (!cComment.equals(other.cComment))
			return false;
		if (cDimCode == null) {
			if (other.cDimCode != null)
				return false;
		} else if (!cDimCode.equals(other.cDimCode))
			return false;
		if (cFactTableColumn == null) {
			if (other.cFactTableColumn != null)
				return false;
		} else if (!cFactTableColumn.equals(other.cFactTableColumn))
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
		if (cMetaDataXML == null) {
			if (other.cMetaDataXML != null)
				return false;
		} else if (!cMetaDataXML.equals(other.cMetaDataXML))
			return false;
		if (cName == null) {
			if (other.cName != null)
				return false;
		} else if (!cName.equals(other.cName))
			return false;
		if (cOperator == null) {
			if (other.cOperator != null)
				return false;
		} else if (!cOperator.equals(other.cOperator))
			return false;
		if (cPath == null) {
			if (other.cPath != null)
				return false;
		} else if (!cPath.equals(other.cPath))
			return false;
		if (cSymbol == null) {
			if (other.cSymbol != null)
				return false;
		} else if (!cSymbol.equals(other.cSymbol))
			return false;
		if (cSynonymCd == null) {
			if (other.cSynonymCd != null)
				return false;
		} else if (!cSynonymCd.equals(other.cSynonymCd))
			return false;
		if (cTableName == null) {
			if (other.cTableName != null)
				return false;
		} else if (!cTableName.equals(other.cTableName))
			return false;
		if (cToolTip == null) {
			if (other.cToolTip != null)
				return false;
		} else if (!cToolTip.equals(other.cToolTip))
			return false;
		if (cTotalNum == null) {
			if (other.cTotalNum != null)
				return false;
		} else if (!cTotalNum.equals(other.cTotalNum))
			return false;
		if (cVisualAttributes == null) {
			if (other.cVisualAttributes != null)
				return false;
		} else if (!cVisualAttributes.equals(other.cVisualAttributes))
			return false;
		if (downloadDate == null) {
			if (other.downloadDate != null)
				return false;
		} else if (!downloadDate.equals(other.downloadDate))
			return false;
		if (i2b2Id == null) {
			if (other.i2b2Id != null)
				return false;
		} else if (!i2b2Id.equals(other.i2b2Id))
			return false;
		if (importDate == null) {
			if (other.importDate != null)
				return false;
		} else if (!importDate.equals(other.importDate))
			return false;
		if (mAppliedPath == null) {
			if (other.mAppliedPath != null)
				return false;
		} else if (!mAppliedPath.equals(other.mAppliedPath))
			return false;
		if (mExclusionCd == null) {
			if (other.mExclusionCd != null)
				return false;
		} else if (!mExclusionCd.equals(other.mExclusionCd))
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
		if (valueTypeCd == null) {
			if (other.valueTypeCd != null)
				return false;
		} else if (!valueTypeCd.equals(other.valueTypeCd))
			return false;
		return true;
	}
	
}
