package etl.data.export.entities.i2b2.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConceptCountsBuilderPaths {
	private String conceptPath;
	
	private Set<String> conceptCds = new HashSet<String>();

	public String getConceptPath() {
		return conceptPath;
	}

	public void setConceptPath(String conceptPath) {
		this.conceptPath = conceptPath;
	}

	public Set<String> getConceptCds() {
		return conceptCds;
	}

	public void setConceptCds(Set<String> conceptCds) {
		this.conceptCds = conceptCds;
	}
	
	
}
