package etl.job.entity.counts;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import etl.job.entity.i2b2tm.ObservationFact;

public class CountNode {
	private String nodeName = "";
	private Set<String> conceptCds = new HashSet<String>();
	private Set<String> patientNums = new HashSet<String>();
	
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public Set<String> getConceptCds() {
		return conceptCds;
	}
	public void setConceptCds(Set<String> conceptCds) {
		this.conceptCds = conceptCds;
	}
	public Set<String> getPatientNums() {
		return patientNums;
	}
	public void setPatientNums(Set<String> patientNums) {
		this.patientNums = patientNums;
	}
	
	public static void gatherPatientNums(Collection<ObservationFact> facts) {

		Map<String,Set<String>> map = 
			facts
			.stream()
			.collect(
				Collectors.groupingBy(ObservationFact::getConceptCd,
						Collectors.mapping(ObservationFact::getPatientNum, Collectors.toSet())
			)
		);
		
		
	}
	
}
