package etl.job.entity.i2b2tm;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.CsvBindByPosition;

public class I2B2Secure implements Cloneable{
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
	private String mAppliedPath;
	@CsvBindByPosition(position = 17)
	private String updateDate;
	@CsvBindByPosition(position = 18)
	private String downloadDate;
	@CsvBindByPosition(position = 19)
	private String importDate;
	@CsvBindByPosition(position = 20)
	private String sourceSystemCd;
	@CsvBindByPosition(position = 21)
	private String valueTypeCd;
	@CsvBindByPosition(position = 22)
	private String mExclusionCd;
	@CsvBindByPosition(position = 23)
	private String cSymbol;
	@CsvBindByPosition(position = 24)
	private String cPath;
	@CsvBindByPosition(position = 25)
	private String i2b2Id;
	@CsvBindByPosition(position = 26)
	private String secureObjToken;
		
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	
	
	public I2B2Secure(I2B2 i2b2) {
		super();
		this.cHlevel = i2b2.getcHlevel();
		this.cFullName = i2b2.getcFullName();
		this.cName = i2b2.getcName();
		this.cSynonymCd = i2b2.getcSynonymCd();
		this.cVisualAttributes = i2b2.getcVisualAttributes();
		this.cTotalNum = i2b2.getcTotalNum();
		this.cBaseCode = i2b2.getcBaseCode();
		this.cMetaDataXML = i2b2.getcMetaDataXML();
		this.cFactTableColumn = i2b2.getcFactTableColumn();
		this.cTableName = i2b2.getcTableName();
		this.cColumnName = i2b2.getcColumnName();
		this.cColumnDataType = i2b2.getcColumnDataType();
		this.cOperator = i2b2.getcOperator();
		this.cDimCode = i2b2.getcDimCode();
		this.cComment = i2b2.getcComment();
		this.cToolTip = i2b2.getcToolTip();
		this.mAppliedPath = i2b2.getmAppliedPath();
		this.updateDate = i2b2.getUpdateDate();
		this.downloadDate = i2b2.getDownloadDate();
		this.importDate = i2b2.getImportDate();
		this.sourceSystemCd = i2b2.getSourceSystemCd();
		this.valueTypeCd = i2b2.getValueTypeCd();
		this.mExclusionCd = i2b2.getmExclusionCd();
		this.cSymbol = i2b2.getcSymbol();
		this.cPath = i2b2.getcPath();
		this.i2b2Id = i2b2.getI2b2Id();
		this.secureObjToken = "EXP:PUBLIC";
	}

	public I2B2Secure() {

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



	public String getmAppliedPath() {
		return mAppliedPath;
	}



	public void setmAppliedPath(String mAppliedPath) {
		this.mAppliedPath = mAppliedPath;
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



	public String getmExclusionCd() {
		return mExclusionCd;
	}



	public void setmExclusionCd(String mExclusionCd) {
		this.mExclusionCd = mExclusionCd;
	}



	public String getcSymbol() {
		return cSymbol;
	}



	public void setcSymbol(String cSymbol) {
		this.cSymbol = cSymbol;
	}



	public String getcPath() {
		return cPath;
	}



	public void setcPath(String cPath) {
		this.cPath = cPath;
	}



	public String getI2b2Id() {
		return i2b2Id;
	}



	public void setI2b2Id(String i2b2Id) {
		this.i2b2Id = i2b2Id;
	}



	public String getSecureObjToken() {
		return secureObjToken;
	}



	public void setSecureObjToken(String secureObjToken) {
		this.secureObjToken = secureObjToken;
	}



	/**
	 * Fill in tree by taking all clevel where the most diversity occurs base nodes then recursively back fill them
	 *  
	 * @param nodes
	 * @throws Exception
	 */
	public static void fillTree(Set<I2B2Secure> nodes, int clevelBegOccurance, int clevelEndOccurance) throws Exception{
		
		Set<I2B2Secure> set = new HashSet<I2B2Secure>();
		/*
		ConcurrentHashMap<CharSequence,Set<CharSequence>> trees = new ConcurrentHashMap<CharSequence, Set<CharSequence>>();
		
		nodes.forEach(node ->{
			
			CharSequence cfullname = node.getcFullName();
			if(StringUtils.countMatches(cfullname, "\\") <= 2) return; // is empty node or only base node.  No need to fill.

			int clevelBegIndex = StringUtils.ordinalIndexOf(cfullname, "\\", clevelBegOccurance);
			
			int clevelEndIndex = StringUtils.ordinalIndexOf(cfullname, "\\", clevelEndOccurance);
			
			CharSequence clevelName = cfullname.subSequence(clevelBegIndex, clevelEndIndex + 1);
			
			if(trees.containsKey(clevelName)) {
				trees.get(clevelName).add(cfullname);
			} else {
				trees.put(clevelName, new HashSet<CharSequence>(Arrays.asList(cfullname)));
			}
			
		});
		
		trees.entrySet().parallelStream()
		*/
		
		nodes.forEach(node ->{
			
			Integer x = StringUtils.countMatches(node.getcFullName(),"\\") - 1;
					
			while(x > 1){
				
				I2B2Secure i2b2 = null;
				try {
					i2b2 = (I2B2Secure) node.clone();
				} catch (CloneNotSupportedException e) {
					System.err.println(e);
				}
				if(i2b2 == null) {
					break;
				}
				
				i2b2.setcFullName(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), "\\", x) + 1 ));
				
				i2b2.setcDimCode(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), "\\", x) + 1 ));
				
				i2b2.setcToolTip(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), "\\", x) + 1 ));
				
				i2b2.setcHlevel(new Integer(x - 2).toString());
				i2b2.setcBaseCode(null);
				i2b2.setcVisualAttributes("FA");
				
				i2b2.setcMetaDataXML("");
				
				String[] fullNodes = i2b2.getcFullName().split("\\\\");
				
				i2b2.setcName(fullNodes[fullNodes.length - 1]);
				
				if(node.getcHlevel() != null) {
					set.add(i2b2);
				}
				
				
				
				x--;
			}
		});
		
		nodes.addAll(set);
	}



	@Override
	public String toString() {
		return "I2B2Secure [cHlevel=" + cHlevel + ", cFullName=" + cFullName + ", cName=" + cName + ", cSynonymCd="
				+ cSynonymCd + ", cVisualAttributes=" + cVisualAttributes + ", cTotalNum=" + cTotalNum + ", cBaseCode="
				+ cBaseCode + ", cMetaDataXML=" + cMetaDataXML + ", cFactTableColumn=" + cFactTableColumn
				+ ", cTableName=" + cTableName + ", cColumnName=" + cColumnName + ", cColumnDataType=" + cColumnDataType
				+ ", cOperator=" + cOperator + ", cDimCode=" + cDimCode + ", cComment=" + cComment + ", cToolTip="
				+ cToolTip + ", mAppliedPath=" + mAppliedPath + ", updateDate=" + updateDate + ", downloadDate="
				+ downloadDate + ", importDate=" + importDate + ", sourceSystemCd=" + sourceSystemCd + ", valueTypeCd="
				+ valueTypeCd + ", mExclusionCd=" + mExclusionCd + ", cSymbol=" + cSymbol + ", cPath=" + cPath
				+ ", i2b2Id=" + i2b2Id + ", secureObjToken=" + secureObjToken + "]";
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cBaseCode == null) ? 0 : cBaseCode.hashCode());
		result = prime * result + ((cColumnDataType == null) ? 0 : cColumnDataType.hashCode());
		result = prime * result + ((cColumnName == null) ? 0 : cColumnName.hashCode());
		result = prime * result + ((cComment == null) ? 0 : cComment.hashCode());
		result = prime * result + ((cDimCode == null) ? 0 : cDimCode.hashCode());
		result = prime * result + ((cFactTableColumn == null) ? 0 : cFactTableColumn.hashCode());
		result = prime * result + ((cFullName == null) ? 0 : cFullName.hashCode());
		result = prime * result + ((cHlevel == null) ? 0 : cHlevel.hashCode());
		result = prime * result + ((cMetaDataXML == null) ? 0 : cMetaDataXML.hashCode());
		result = prime * result + ((cName == null) ? 0 : cName.hashCode());
		result = prime * result + ((cOperator == null) ? 0 : cOperator.hashCode());
		result = prime * result + ((cPath == null) ? 0 : cPath.hashCode());
		result = prime * result + ((cSymbol == null) ? 0 : cSymbol.hashCode());
		result = prime * result + ((cSynonymCd == null) ? 0 : cSynonymCd.hashCode());
		result = prime * result + ((cTableName == null) ? 0 : cTableName.hashCode());
		result = prime * result + ((cToolTip == null) ? 0 : cToolTip.hashCode());
		result = prime * result + ((cTotalNum == null) ? 0 : cTotalNum.hashCode());
		result = prime * result + ((cVisualAttributes == null) ? 0 : cVisualAttributes.hashCode());
		result = prime * result + ((downloadDate == null) ? 0 : downloadDate.hashCode());
		result = prime * result + ((i2b2Id == null) ? 0 : i2b2Id.hashCode());
		result = prime * result + ((importDate == null) ? 0 : importDate.hashCode());
		result = prime * result + ((mAppliedPath == null) ? 0 : mAppliedPath.hashCode());
		result = prime * result + ((mExclusionCd == null) ? 0 : mExclusionCd.hashCode());
		result = prime * result + ((secureObjToken == null) ? 0 : secureObjToken.hashCode());
		result = prime * result + ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
		result = prime * result + ((updateDate == null) ? 0 : updateDate.hashCode());
		result = prime * result + ((valueTypeCd == null) ? 0 : valueTypeCd.hashCode());
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
		I2B2Secure other = (I2B2Secure) obj;
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
		if (secureObjToken == null) {
			if (other.secureObjToken != null)
				return false;
		} else if (!secureObjToken.equals(other.secureObjToken))
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
