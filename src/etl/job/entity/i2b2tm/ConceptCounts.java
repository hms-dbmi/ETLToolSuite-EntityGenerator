package etl.job.entity.i2b2tm;

import com.opencsv.bean.CsvBindByPosition;

public class ConceptCounts {
	@CsvBindByPosition(position = 0)
	private String conceptPath;
	@CsvBindByPosition(position = 1)
	private String parentConceptPath;
	@CsvBindByPosition(position = 2)
	private Integer patientCount;
	
	public ConceptCounts() {
		// TODO Auto-generated constructor stub
	}
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}

	public String getConceptPath() {
		return conceptPath;
	}

	public void setConceptPath(String conceptPath) {
		this.conceptPath = conceptPath;
	}

	public String getParentConceptPath() {
		return parentConceptPath;
	}

	public void setParentConceptPath(String parentConceptPath) {
		this.parentConceptPath = parentConceptPath;
	}

	public Integer getPatientCount() {
		return patientCount;
	}

	public void setPatientCount(Integer patientCount) {
		this.patientCount = patientCount;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((conceptPath == null) ? 0 : conceptPath.hashCode());
		result = prime * result + ((parentConceptPath == null) ? 0 : parentConceptPath.hashCode());
		result = prime * result + ((patientCount == null) ? 0 : patientCount.hashCode());
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
		ConceptCounts other = (ConceptCounts) obj;
		if (conceptPath == null) {
			if (other.conceptPath != null)
				return false;
		} else if (!conceptPath.equals(other.conceptPath))
			return false;
		if (parentConceptPath == null) {
			if (other.parentConceptPath != null)
				return false;
		} else if (!parentConceptPath.equals(other.parentConceptPath))
			return false;
		if (patientCount == null) {
			if (other.patientCount != null)
				return false;
		} else if (!patientCount.equals(other.patientCount))
			return false;
		return true;
	}
	
}
