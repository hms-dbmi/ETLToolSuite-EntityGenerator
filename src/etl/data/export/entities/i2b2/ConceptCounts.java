package etl.data.export.entities.i2b2;

import java.util.ArrayList;
import java.util.List;

import etl.data.export.entities.Entity;
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
		// TODO Auto-generated method stub
		return null;
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

	public List<ConceptCounts> generateCounts(List<Entity> entities) throws Exception{
		
		List<I2B2> i2b2s = new ArrayList<I2B2>();
		
		List<ConceptDimension> concDims = new ArrayList<ConceptDimension>();
		
		List<ObservationFact> obsFacts = new ArrayList<ObservationFact>();
		
		for(Entity ent: entities) {
			
			if(ent instanceof I2B2) {
				
				i2b2s.add((I2B2) ent);
				
			} else if(ent instanceof ConceptDimension) {
				
				concDims.add((ConceptDimension) ent);
				
			} else if(ent instanceof ObservationFact) {
				
				obsFacts.add((ObservationFact) ent);
				
			}
			
		}
		
		List<ConceptCounts> counts = new ArrayList<ConceptCounts>();
		
		for(I2B2 i2b2: i2b2s) {
			
			ConceptCounts cc = new ConceptCounts("ConceptCounts");
			
			
			
		}
		
		
		return counts;
		
	}
	
}
