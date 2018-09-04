package etl.data.export.entities.i2b2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multiset.Entry;

import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.utils.ConceptCountsBuilder;
import etl.data.export.entities.i2b2.utils.ConceptCountsBuilderPaths;
import etl.mapping.CsvToI2b2TMMapping;

public class ConceptCounts extends Entity {
	private String conceptPath;
	private String parentConceptPath;
	private Integer patientCount;
	

	public ConceptCounts(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping, String[] data) {
		// TODO Auto-generated method stub

	}
	
	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toCsv() {
		return 	
				makeStringSafe(conceptPath) + "," +
				makeStringSafe(parentConceptPath) + "," +
				patientCount;	
		
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

	public static List<ConceptCounts> generateCounts2(Set<Entity> entities) throws Exception{

		List<ConceptCounts> cc = new ArrayList<ConceptCounts>();
		
		Map<String, ConceptCountsBuilder> conceptsPatients = new HashMap<String, ConceptCountsBuilder>();
		
		Map<String, ConceptCountsBuilderPaths> pathsConcepts = new HashMap<String, ConceptCountsBuilderPaths>();		
		
		Map<String, String> conceptPaths = new HashMap<String,String>();
		
		for(Entity ent: entities) {
			
			if(ent instanceof ObservationFact) {
				
				if(conceptsPatients.containsKey(((ObservationFact) ent).getConceptCd())) {
					
					ConceptCountsBuilder ccb = conceptsPatients.get(((ObservationFact) ent).getConceptCd());
					
					Set<String> patients =  ccb.getPatients();
					
					patients.add(((ObservationFact) ent).getPatientNum());
					
					ccb.setPatients(patients);
					
					conceptsPatients.put(((ObservationFact) ent).getConceptCd(), ccb);
					
				} else {
					
					ConceptCountsBuilder ccb = new ConceptCountsBuilder();
					
					Set<String> patients =  ccb.getPatients();
					
					patients.add(((ObservationFact) ent).getPatientNum());
					
					ccb.setPatients(patients);
					
					conceptsPatients.put(((ObservationFact) ent).getConceptCd(), ccb);
				}
				
			} else if(ent instanceof I2B2) {
				
				if(!pathsConcepts.containsKey(((I2B2) ent).getcFullName())) {					
					
					ConceptCountsBuilderPaths ccbp = new ConceptCountsBuilderPaths();
					
					ccbp.setConceptPath(((I2B2) ent).getcFullName());
					
					pathsConcepts.put(((I2B2) ent).getcFullName(), ccbp);
					
				}
			} else if(ent instanceof ConceptDimension) {
				
				conceptPaths.put(((ConceptDimension) ent).getConceptPath(), ((ConceptDimension) ent).getConceptCd());
				
			}
		} 
		
		Map<String,Set<String>> nodeConcepts = new HashMap<String,Set<String>>();
		
		for(java.util.Map.Entry<String, String> e: conceptPaths.entrySet()) {
			
			recurseCounts(e.getKey(),e.getValue(),nodeConcepts);
			
		}
		
		Map<String,Set<String>> patientSets = new HashMap<String,Set<String>>();
		
		for(String key: nodeConcepts.keySet()) {
			Set<String> patientIds = new HashSet<String>();
			
			Set<String> conceptkeys = nodeConcepts.get(key);
			
			for(String conceptkey: conceptkeys) {
				
				if(conceptsPatients.containsKey(conceptkey)) {
				
					ConceptCountsBuilder ccb = conceptsPatients.get(conceptkey);
					
					patientIds.addAll(ccb.getPatients());
				}
			}
			int size = patientIds.size();
			
			ConceptCounts nodescount = new ConceptCounts("ConceptCounts");
			
			nodescount.setConceptPath(key);
			
			int endIndex = org.apache.commons.lang3.StringUtils.ordinalIndexOf(
						key, 
						"\\", 
						org.apache.commons.lang3.StringUtils.countMatches(key, "\\") - 1
					) + 1;
			
			nodescount.setParentConceptPath(key.substring(0, endIndex));
			nodescount.setPatientCount(patientIds.size());
			
			cc.add(nodescount);
			
		}
		
		return cc;
	}

	private static void recurseCounts(String key, String value, Map<String, Set<String>> map) {
		if(map.containsKey(key)) {
			Set<String> set = map.get(key);
			set.add(value);
			map.put(key, set);
		} else {
			Set<String> set = new HashSet<String>();
			
			set.add(value);
			
			map.put(key, set);
			
		}
		
		int nodecount = org.apache.commons.lang3.StringUtils.countMatches(key, "\\") - 1;
		
		if(nodecount != 1) {
			
			int index = org.apache.commons.lang3.StringUtils.ordinalIndexOf(key, "\\", nodecount);
			
			String node = key.substring(0, index + 1);

			recurseCounts(node,value,map);
		}
		
	}

	@Deprecated
	public List<ConceptCounts> generateCounts(Set<Entity> entities) throws Exception{
		
		List<ConceptCounts> counts = new ArrayList<ConceptCounts>();
		
		List<I2B2> i2b2s = new ArrayList<I2B2>();
		
		List<ConceptDimension> concDims = new ArrayList<ConceptDimension>();

		Map<String,List<String>> patientSets = new HashMap<String,List<String>>();
		
		for(Entity ent: entities) {
			
			if(ent instanceof I2B2) {
				
				i2b2s.add((I2B2) ent);
				
			} else if(ent instanceof ConceptDimension) {
				
				concDims.add((ConceptDimension) ent);
				
			} else if(ent instanceof ObservationFact) {
				
				ObservationFact of = (ObservationFact) ent;

				if(patientSets.containsKey(of.getConceptCd())) {

					List<String> patSet = patientSets.get(of.getConceptCd());
					
					if(!patSet.contains(of.getPatientNum())) {
						
						patSet.add(of.getPatientNum());
						patientSets.put(of.getConceptCd(), patSet);
						
					}
					
				} else {
					
					List<String> patSet = new ArrayList<String>();
					patSet.add(of.getPatientNum());
					patientSets.put(of.getConceptCd(), patSet);
										
				}
				 
			}
			
		}
				
		for(I2B2 i2b2: i2b2s) {
			
			if(i2b2.getcFullName() != null || !i2b2.getcFullName().isEmpty()) {
				
				String node = i2b2.getcFullName();
				
				int slashes = org.apache.commons.lang3.StringUtils.countMatches(node, "\\") - 1;
				
				int endIndex = org.apache.commons.lang3.StringUtils.ordinalIndexOf(node,"\\", slashes) + 1;
				
				String parentNode = i2b2.getcFullName().substring(0, endIndex);
				
				List<String> nodesPats = new ArrayList<String>();
				
				for(ConceptDimension cd: concDims) {
					String conceptPath = cd.getConceptPath();
					
					if(node.length() < conceptPath.length()) {
						String conceptPathResized = conceptPath.substring(0, node.length() - 1);
						
						if(node.equals(conceptPathResized)) {
							
							List<String> patients = patientSets.get(cd.getConceptCd());
							
							for(String pat: patients) {
								
								if(!nodesPats.contains(pat)) {
									
									nodesPats.add(pat);
									
								}
								
							}
							
						}
						
					}
					
				}
				
				ConceptCounts cc = new ConceptCounts("ConceptCounts");
				
				cc.setConceptPath(node);
				
				cc.setParentConceptPath(parentNode);
				
				cc.setPatientCount(nodesPats.size());

				counts.add(cc); 
			}
		}
		
		return counts;
		
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
