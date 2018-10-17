package etl.data.export.entities.i2b2.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.ObjectMapping;
import etl.data.export.entities.i2b2.PatientDimension;
import etl.data.export.entities.i2b2.PatientMapping;

public class ColumnSequencer {
	public List<String> entityNames = new ArrayList<String>();
	public String entityColumn;
	public String columnType;
	public String mappedName;
	public int startSequence;
	public int interval = 1;
	public int currSequence;
	public ArrayList<String> values = new ArrayList<String>();
	private Map<String,Integer> sequenced = new HashMap<String,Integer>();
	public boolean IS_CONSTANT; 

	
	public Set<PatientMapping> generateSeqeunce2(Set<Entity> builtEnts) throws Exception{
		 //List<String> seq = new ArrayList<String>();
		Set<PatientMapping> patientMappings = new HashSet<PatientMapping>();
		
		/*
		ConcurrentMap<String, List<Entity>> ents = builtEnts.parallelStream().collect(Collectors.groupingByConcurrent(Entity::getEntityType));
		
		Set<String> distinctValues = builtEnts.stream().filter(e -> e.getEntityType().equals("ConceptDimension")).map(ConceptDimension::getConceptCd);;
		
		//Set<String> distinctValues = new HashSet<String>();
		
		if(ents.containsKey("ConceptDimension")) {
			
			for(Entity ent:ents.get("ConceptDimension")) {
				
				ConceptDimension cd = (ConceptDimension) ent;
				
				distinctValues.add(cd.getConceptCd());
			}
			
		}*/
		for(Entity entity: builtEnts) {
			if(entity instanceof PatientDimension) {
				if(entityNames.indexOf(entity.getClass().getSimpleName()) != -1) {
					
					Field field = entity.getClass().getDeclaredField(this.entityColumn);	
					field.setAccessible(true);
					Object sourceId = field.get(entity);
					if(sourceId != null) {
						int index = findSeqId(sourceId.toString());
							
						field.set(entity, new Integer(index).toString());
						PatientMapping pm = new PatientMapping("PatientMapping");
						
						pm.setPatientNum(new Integer(index).toString());
						pm.setPatientIdeSource(sourceId.toString());
						pm.setPatientIde(this.columnType + ":" + this.entityColumn + ":" + this.mappedName);
						patientMappings.add(pm);
						
						field.setAccessible(false);
					}
				}
			}
		}
		return patientMappings;
	}
	

	
	public Set<ObjectMapping> generateSeqeunce(Set<Entity> builtEnts) throws Exception{
		 //List<String> seq = new ArrayList<String>();
		Set<ObjectMapping> objectMappings = new HashSet<ObjectMapping>();
		
		/*
		ConcurrentMap<String, List<Entity>> ents = builtEnts.parallelStream().collect(Collectors.groupingByConcurrent(Entity::getEntityType));
		
		Set<String> distinctValues = builtEnts.stream().filter(e -> e.getEntityType().equals("ConceptDimension")).map(ConceptDimension::getConceptCd);;
		
		//Set<String> distinctValues = new HashSet<String>();
		
		if(ents.containsKey("ConceptDimension")) {
			
			for(Entity ent:ents.get("ConceptDimension")) {
				
				ConceptDimension cd = (ConceptDimension) ent;
				
				distinctValues.add(cd.getConceptCd());
			}
			
		}*/
		for(Entity entity: builtEnts) {
			if(entityNames.indexOf(entity.getClass().getSimpleName()) != -1) {
				
				Field field = entity.getClass().getDeclaredField(this.entityColumn);	
				field.setAccessible(true);
				Object sourceId = field.get(entity);
				if(sourceId != null) {
					int index = findSeqId(sourceId.toString());
						
					field.set(entity, new Integer(index).toString());
					ObjectMapping om = new ObjectMapping("ObjectMapping");
					
					om.setMappedId(new Integer(index).toString());
					om.setSourceId(sourceId.toString());
					om.setSourceName(this.columnType + ":" + this.entityColumn + ":" + this.mappedName);
					objectMappings.add(om);
					
					field.setAccessible(false);
				}
			}
		}
		return objectMappings;
	}
	
	public Integer findSeqId(String searchfield) {
		
		Integer index;
		if(IS_CONSTANT) {
			return -1;
		}
		if(!this.sequenced.containsKey(searchfield)) {
			
			index = this.currSequence;
			
			this.sequenced.put(searchfield, index);
			
			this.currSequence = this.currSequence + this.interval;
			
		} else {
			
			index = this.sequenced.get(searchfield);
			
		}
		
		//System.out.println(this.values.indexOf(searchfield));
		/*
		if(this.values.indexOf(searchfield) < 0) {

			index = this.currSequence;
			this.currSequence = this.currSequence + this.interval;
			
			this.values.add(searchfield);

		} else {
			index = this.values.indexOf(searchfield) + startSequence;
		}*/
		return index;
		
	}
	public ColumnSequencer(List<String> entityNames, String entityColumn,String columnType, int startSequence) {
		super();
		this.entityNames = entityNames;
		this.entityColumn = entityColumn;
		this.startSequence = startSequence;
		this.currSequence = startSequence;
		this.columnType = columnType;
	}
	
	public ColumnSequencer(List<String> entityNames, String entityColumn, String columnType, String mappedName, int startSequence, int interval) {
		super();
		this.entityNames = entityNames;
		this.entityColumn = entityColumn;
		this.startSequence = startSequence;
		this.interval = interval;
		this.currSequence = startSequence;
		this.mappedName = mappedName;
		this.columnType = columnType;
	}	
	
	public ColumnSequencer(List<String> entityNames, String entityColumn,String columnType, int startSequence, boolean isConstant) {
		super();
		this.entityNames = entityNames;
		this.entityColumn = entityColumn;
		this.startSequence = startSequence;
		this.currSequence = startSequence;
		this.columnType = columnType;
		this.IS_CONSTANT = isConstant;
	}
	
	public ColumnSequencer(List<String> entityNames, String entityColumn, String columnType, String mappedName, int startSequence, int interval, boolean isConstant) {
		super();
		this.entityNames = entityNames;
		this.entityColumn = entityColumn;
		this.startSequence = startSequence;
		this.interval = interval;
		this.currSequence = startSequence;
		this.mappedName = mappedName;
		this.columnType = columnType;
		this.IS_CONSTANT = isConstant;
	}

	public Integer nextVal() {
		this.currSequence = this.currSequence + this.interval;
		return new Integer(this.currSequence);
		
	}
	

}
