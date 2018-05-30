package etl.data.export.entities.i2b2.utils;

import java.util.HashSet;
import java.util.Set;

public class ConceptCountsBuilder {
	private String conceptCd;
	
	private Set<String> patients = new HashSet<String>();

	public String getConceptCd() {
		return conceptCd;
	}

	public void setConceptCd(String conceptCd) {
		this.conceptCd = conceptCd;
	}

	public Set<String> getPatients() {
		return patients;
	}

	public void setPatients(Set<String> patients) {
		this.patients = patients;
	}
	
	
}
