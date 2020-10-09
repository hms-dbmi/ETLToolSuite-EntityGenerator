package etl.job.entity.hpds;

public class AllConcepts {
	private Integer patientNum;
	private String conceptPath;
	private String tvalChar;
	private String nvalNum;
	private String startDate = "0";
	
	public Integer getPatientNum() {
		return patientNum;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public void setPatientNum(Integer patientNum) {
		this.patientNum = patientNum;
	}
	public String getConceptPath() {
		return conceptPath;
	}
	public void setConceptPath(String conceptPath) {
		this.conceptPath = conceptPath;
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
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((conceptPath == null) ? 0 : conceptPath.hashCode());
		result = prime * result + ((patientNum == null) ? 0 : patientNum.hashCode());
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
		AllConcepts other = (AllConcepts) obj;
		if (conceptPath == null) {
			if (other.conceptPath != null)
				return false;
		} else if (!conceptPath.equals(other.conceptPath))
			return false;
		if (patientNum == null) {
			if (other.patientNum != null)
				return false;
		} else if (!patientNum.equals(other.patientNum))
			return false;
		
		return true;
	}
	
	@Override
	public String toString() {
		return "AllConcepts [patientNum=" + patientNum + ", conceptPath=" + conceptPath + ", tvalChar=" + tvalChar
				+ ", nvalNum=" + nvalNum + "]";
	}
	public boolean isValid() {
		
		if(patientNum == null) return false;
		//if(Integer.isEmpty) return false;
		if(conceptPath == null) return false;
		if(tvalChar.equals("null")) return false;
		if(nvalNum.equals("null")) return false;
		if(conceptPath.trim().isEmpty()) return false;
		
		return true;
	}
	
	public char[] toCSV() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append('"');
		sb.append(patientNum);
		sb.append("\",\"");
		sb.append(conceptPath);
		sb.append("\",\"");
		sb.append(nvalNum);
		sb.append("\",\"");
		sb.append(tvalChar);
		sb.append("\",\"");
		sb.append(startDate);
		sb.append('"');
		sb.append('\n');
		
		return sb.toString().toCharArray();
		
	}
	
	
}
