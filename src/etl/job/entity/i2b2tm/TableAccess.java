package etl.job.entity.i2b2tm;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.opencsv.bean.CsvBindByPosition;

public class TableAccess {
	@CsvBindByPosition(position = 0)
	private String valuetypeCd;
	@CsvBindByPosition(position = 1)
	private String cStatusCd;
	@CsvBindByPosition(position = 2)
	private String cChangeDate;
	@CsvBindByPosition(position = 3)
	private String cEntryDate;
	@CsvBindByPosition(position = 4)
	private String cTooltip;
	@CsvBindByPosition(position = 5)
	private String cComment;
	@CsvBindByPosition(position = 6)
	private String cDimcode;
	@CsvBindByPosition(position = 7)
	private String cOperator;
	@CsvBindByPosition(position = 8)
	private String cColumndatatype;
	@CsvBindByPosition(position = 9)
	private String cColumnname;
	@CsvBindByPosition(position = 10)
	private String cDimtablename;
	@CsvBindByPosition(position = 11)
	private String cFacttablecolumn;
	@CsvBindByPosition(position = 12)
	private String cMetadataxml;
	@CsvBindByPosition(position = 13)
	private String cBasecode;
	@CsvBindByPosition(position = 14)
	private String cTotalnum;
	@CsvBindByPosition(position = 15)
	private String cVisualattributes;
	@CsvBindByPosition(position = 16)
	private String cSynonymCd;
	@CsvBindByPosition(position = 17)
	private String cName;
	@CsvBindByPosition(position = 18)
	private String cFullname;
	@CsvBindByPosition(position = 19)
	private String cHlevel;
	@CsvBindByPosition(position = 20)
	private String cProtectedAccess;
	@CsvBindByPosition(position = 21)
	private String cTableName;
	@CsvBindByPosition(position = 22)
	private String cTableCd;
	
	
	
	public String getValuetypeCd() {
		return valuetypeCd;
	}

	public void setValuetypeCd(String valuetypeCd) {
		this.valuetypeCd = valuetypeCd;
	}

	public String getcStatusCd() {
		return cStatusCd;
	}

	public void setcStatusCd(String cStatusCd) {
		this.cStatusCd = cStatusCd;
	}

	public String getcChangeDate() {
		return cChangeDate;
	}

	public void setcChangeDate(String cChangeDate) {
		this.cChangeDate = cChangeDate;
	}

	public String getcEntryDate() {
		return cEntryDate;
	}

	public void setcEntryDate(String cEntryDate) {
		this.cEntryDate = cEntryDate;
	}

	public String getcTooltip() {
		return cTooltip;
	}

	public void setcTooltip(String cTooltip) {
		this.cTooltip = cTooltip;
	}

	public String getcComment() {
		return cComment;
	}

	public void setcComment(String cComment) {
		this.cComment = cComment;
	}

	public String getcDimcode() {
		return cDimcode;
	}

	public void setcDimcode(String cDimcode) {
		this.cDimcode = cDimcode;
	}

	public String getcOperator() {
		return cOperator;
	}

	public void setcOperator(String cOperator) {
		this.cOperator = cOperator;
	}

	public String getcColumndatatype() {
		return cColumndatatype;
	}

	public void setcColumndatatype(String cColumndatatype) {
		this.cColumndatatype = cColumndatatype;
	}

	public String getcColumnname() {
		return cColumnname;
	}

	public void setcColumnname(String cColumnname) {
		this.cColumnname = cColumnname;
	}

	public String getcDimtablename() {
		return cDimtablename;
	}

	public void setcDimtablename(String cDimtablename) {
		this.cDimtablename = cDimtablename;
	}

	public String getcFacttablecolumn() {
		return cFacttablecolumn;
	}

	public void setcFacttablecolumn(String cFacttablecolumn) {
		this.cFacttablecolumn = cFacttablecolumn;
	}

	public String getcMetadataxml() {
		return cMetadataxml;
	}

	public void setcMetadataxml(String cMetadataxml) {
		this.cMetadataxml = cMetadataxml;
	}

	public String getcBasecode() {
		return cBasecode;
	}

	public void setcBasecode(String cBasecode) {
		this.cBasecode = cBasecode;
	}

	public String getcTotalnum() {
		return cTotalnum;
	}

	public void setcTotalnum(String cTotalnum) {
		this.cTotalnum = cTotalnum;
	}

	public String getcVisualattributes() {
		return cVisualattributes;
	}

	public void setcVisualattributes(String cVisualattributes) {
		this.cVisualattributes = cVisualattributes;
	}

	public String getcSynonymCd() {
		return cSynonymCd;
	}

	public void setcSynonymCd(String cSynonymCd) {
		this.cSynonymCd = cSynonymCd;
	}

	public String getcName() {
		return cName;
	}

	public void setcName(String cName) {
		this.cName = cName;
	}

	public String getcFullname() {
		return cFullname;
	}

	public void setcFullname(String cFullname) {
		this.cFullname = cFullname;
	}

	public String getcHlevel() {
		return cHlevel;
	}

	public void setcHlevel(String cHlevel) {
		this.cHlevel = cHlevel;
	}

	public String getcProtectedAccess() {
		return cProtectedAccess;
	}

	public void setcProtectedAccess(String cProtectedAccess) {
		this.cProtectedAccess = cProtectedAccess;
	}

	public String getcTableName() {
		return cTableName;
	}

	public void setcTableName(String cTableName) {
		this.cTableName = cTableName;
	}

	public String getcTableCd() {
		return cTableCd;
	}

	public void setcTableCd(String cTableCd) {
		this.cTableCd = cTableCd;
	}

	public TableAccess() {
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cBasecode == null) ? 0 : cBasecode.hashCode());
		result = prime * result + ((cChangeDate == null) ? 0 : cChangeDate.hashCode());
		result = prime * result + ((cColumndatatype == null) ? 0 : cColumndatatype.hashCode());
		result = prime * result + ((cColumnname == null) ? 0 : cColumnname.hashCode());
		result = prime * result + ((cComment == null) ? 0 : cComment.hashCode());
		result = prime * result + ((cDimcode == null) ? 0 : cDimcode.hashCode());
		result = prime * result + ((cDimtablename == null) ? 0 : cDimtablename.hashCode());
		result = prime * result + ((cEntryDate == null) ? 0 : cEntryDate.hashCode());
		result = prime * result + ((cFacttablecolumn == null) ? 0 : cFacttablecolumn.hashCode());
		result = prime * result + ((cFullname == null) ? 0 : cFullname.hashCode());
		result = prime * result + ((cHlevel == null) ? 0 : cHlevel.hashCode());
		result = prime * result + ((cMetadataxml == null) ? 0 : cMetadataxml.hashCode());
		result = prime * result + ((cName == null) ? 0 : cName.hashCode());
		result = prime * result + ((cOperator == null) ? 0 : cOperator.hashCode());
		result = prime * result + ((cProtectedAccess == null) ? 0 : cProtectedAccess.hashCode());
		result = prime * result + ((cStatusCd == null) ? 0 : cStatusCd.hashCode());
		result = prime * result + ((cSynonymCd == null) ? 0 : cSynonymCd.hashCode());
		result = prime * result + ((cTableCd == null) ? 0 : cTableCd.hashCode());
		result = prime * result + ((cTableName == null) ? 0 : cTableName.hashCode());
		result = prime * result + ((cTooltip == null) ? 0 : cTooltip.hashCode());
		result = prime * result + ((cTotalnum == null) ? 0 : cTotalnum.hashCode());
		result = prime * result + ((cVisualattributes == null) ? 0 : cVisualattributes.hashCode());
		result = prime * result + ((valuetypeCd == null) ? 0 : valuetypeCd.hashCode());
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
		TableAccess other = (TableAccess) obj;
		if (cBasecode == null) {
			if (other.cBasecode != null)
				return false;
		} else if (!cBasecode.equals(other.cBasecode))
			return false;
		if (cChangeDate == null) {
			if (other.cChangeDate != null)
				return false;
		} else if (!cChangeDate.equals(other.cChangeDate))
			return false;
		if (cColumndatatype == null) {
			if (other.cColumndatatype != null)
				return false;
		} else if (!cColumndatatype.equals(other.cColumndatatype))
			return false;
		if (cColumnname == null) {
			if (other.cColumnname != null)
				return false;
		} else if (!cColumnname.equals(other.cColumnname))
			return false;
		if (cComment == null) {
			if (other.cComment != null)
				return false;
		} else if (!cComment.equals(other.cComment))
			return false;
		if (cDimcode == null) {
			if (other.cDimcode != null)
				return false;
		} else if (!cDimcode.equals(other.cDimcode))
			return false;
		if (cDimtablename == null) {
			if (other.cDimtablename != null)
				return false;
		} else if (!cDimtablename.equals(other.cDimtablename))
			return false;
		if (cEntryDate == null) {
			if (other.cEntryDate != null)
				return false;
		} else if (!cEntryDate.equals(other.cEntryDate))
			return false;
		if (cFacttablecolumn == null) {
			if (other.cFacttablecolumn != null)
				return false;
		} else if (!cFacttablecolumn.equals(other.cFacttablecolumn))
			return false;
		if (cFullname == null) {
			if (other.cFullname != null)
				return false;
		} else if (!cFullname.equals(other.cFullname))
			return false;
		if (cHlevel == null) {
			if (other.cHlevel != null)
				return false;
		} else if (!cHlevel.equals(other.cHlevel))
			return false;
		if (cMetadataxml == null) {
			if (other.cMetadataxml != null)
				return false;
		} else if (!cMetadataxml.equals(other.cMetadataxml))
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
		if (cProtectedAccess == null) {
			if (other.cProtectedAccess != null)
				return false;
		} else if (!cProtectedAccess.equals(other.cProtectedAccess))
			return false;
		if (cStatusCd == null) {
			if (other.cStatusCd != null)
				return false;
		} else if (!cStatusCd.equals(other.cStatusCd))
			return false;
		if (cSynonymCd == null) {
			if (other.cSynonymCd != null)
				return false;
		} else if (!cSynonymCd.equals(other.cSynonymCd))
			return false;
		if (cTableCd == null) {
			if (other.cTableCd != null)
				return false;
		} else if (!cTableCd.equals(other.cTableCd))
			return false;
		if (cTableName == null) {
			if (other.cTableName != null)
				return false;
		} else if (!cTableName.equals(other.cTableName))
			return false;
		if (cTooltip == null) {
			if (other.cTooltip != null)
				return false;
		} else if (!cTooltip.equals(other.cTooltip))
			return false;
		if (cTotalnum == null) {
			if (other.cTotalnum != null)
				return false;
		} else if (!cTotalnum.equals(other.cTotalnum))
			return false;
		if (cVisualattributes == null) {
			if (other.cVisualattributes != null)
				return false;
		} else if (!cVisualattributes.equals(other.cVisualattributes))
			return false;
		if (valuetypeCd == null) {
			if (other.valuetypeCd != null)
				return false;
		} else if (!valuetypeCd.equals(other.valuetypeCd))
			return false;
		return true;
	}

	public static Collection<? extends TableAccess> createTableAccess(Set<I2B2> metadata) {
		Set<TableAccess> nodes = new HashSet<TableAccess>();
		
		metadata.stream().forEach(node ->{
			if(node.getcHlevel() == null ){
				return;
			};
			if(node == null || node.getcHlevel() == null) {
				metadata.remove(node);
			}
			if(node.getcHlevel().equals("0")) {
				TableAccess ta = new TableAccess();
				ta.setcTableCd(node.getcName());
				ta.setcTableName("I2B2");
				ta.setcProtectedAccess("N");
				ta.setcHlevel(node.getcHlevel());
				ta.setcFullname(node.getcFullName());
				ta.setcName(node.getcName());
				ta.setcSynonymCd("N");
				ta.setcVisualattributes("CA");
				ta.setcTotalnum("0");
				ta.setcFacttablecolumn("CONCEPT_CD");
				ta.setcDimtablename("CONCEPT_DIMENSION");
				ta.setcColumnname("CONCEPT_PATH");
				ta.setcColumndatatype("T");
				ta.setcOperator("LIKE");
				ta.setcDimcode(node.getcDimCode());
				ta.setcTooltip(node.getcToolTip());
				ta.setcStatusCd("A");
				nodes.add(ta);
				
			}
		});
		
		return nodes;
	}

}
