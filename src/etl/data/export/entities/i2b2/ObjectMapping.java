package etl.data.export.entities.i2b2;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.utils.ColumnSequencer;
import etl.mapping.CsvToI2b2TMMapping;

public class ObjectMapping extends Entity {
	private static final Logger logger = LogManager.getLogger(ObjectMapping.class);

	
	private String sourceId; 
	private String sourceName;
	private String mappedId;
	
	public ObjectMapping(String str) throws Exception {
		super(str);
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getMappedId() {
		return mappedId;
	}

	public void setMappedId(String mappedId) {
		this.mappedId = mappedId;
	}

	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping, String[] data) {
		// TODO Auto-generated method stub

	}
	
	
	public Map<String, List<ObjectMapping>> generateObjectMappings(Set<Entity> entities, List<ColumnSequencer> sequencers) throws Exception {
		Map<String, List<ObjectMapping>> objectMappings = new HashMap<String,List<ObjectMapping>>();
		/*
		for(Entity entity: entities) {
			for(ColumnSequencer sequence: sequencers) {
				
				ectMapping om = sequenceColumn(entity,sequence);
				if(om.isValid()) {
					if(objectMappings.containsKey(sequence.entityColumn)) {
						List<ObjectMapping> set = objectMappings.get(sequence.entityColumn);
					
						set.add(om);
						
						objectMappings.put(sequence.entityColumn, set);
					} else {
						List<ObjectMapping> set = new ArrayList<ObjectMapping>();
						
						set.add(om);
						
						objectMappings.put(sequence.entityColumn, set);
					} 					
				}
				
			}
		}
		*/
		return objectMappings;
		
	}
	/*
	public ObjectMapping sequenceColumn(Entity entity, ColumnSequencer sequence) throws Exception {
		ObjectMapping om = new ObjectMapping("ObjectMapping");

		if(entity.getClass().getSimpleName().equalsIgnoreCase(sequence.entityName)) {
			Field field = entity.getClass().getDeclaredField(sequence.entityColumn);	
			
			field.setAccessible(true);
			
			Object sourceId = field.get(entity);
			
			om.setSourceId(sourceId.toString());
			om.setSourceName(sequence.columnType + ":" + sequence.entityColumn + ":" + sequence.mappedName);
			om.setMappedId(sequence.nextVal().toString());
			
			field.setAccessible(false);
		}
		
		return om;
	}
	*/
	@Override
	public boolean isValid() {
		if( sourceId != null || mappedId != null ) return true;
		return false;
	}

	@Override
	public String toCsv() {
		return makeStringSafe(this.sourceId) + "," + makeStringSafe(this.sourceName) + "," 
				+ makeStringSafe(this.mappedId);
	}
	int x = 0;
	public void setMappedSequences(Map<String, List<ObjectMapping>> oms, Set<Entity> builtEnts, List<ColumnSequencer> sequencers) 
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		Map<String, Set<Entity>> entMap = organizeEnts(builtEnts);
		
		List<Entity> entList = new ArrayList(builtEnts);
		/*		
		for(ColumnSequencer sequence: sequencers) {
			if(entMap.containsKey(sequence.entityName)) {
				Set<Entity> entities = entMap.get(sequence.entityName);
				
				logger.info("Starting Sequencing for: " + sequence.entityName + "." + sequence.entityColumn);
				logger.info("Sequencing " + entities.size() + " records");
				
				entities.parallelStream().forEach(entity -> {
					if(oms.containsKey(sequence.entityColumn)) {
						try {
							List<ObjectMapping> omList = oms.get(sequence.entityColumn);
							
							Field field = entity.getClass().getDeclaredField(sequence.entityColumn);
							
							field.setAccessible(true);
							
							Object sourceId = field.get(entity);
							
							ObjectMapping remove = null;
							
							for(ObjectMapping om: omList) {
								if(om.getSourceId().equalsIgnoreCase(sourceId.toString())) {
									remove = om;
									field.set(entity, om.getMappedId());
									break;
								}
							}
							if(remove != null) omList.remove(remove);
							
						} catch (Exception e) {}
						//logger.info(sequence.entityColumn + x++);
						/*
						try {
							
							
							Field field = entity.getClass().getDeclaredField(sequence.entityColumn);
							
							field.setAccessible(true);
							
							Object sourceId = field.get(entity);
							
							int index = omIndex.get(sourceId.toString());
							
							ObjectMapping om = omList.get(index);
							
							if(sourceId.equals(om.sourceId)) {
								
								field.set(entity, om.getMappedId());
								
							}
							
							field.setAccessible(false);
						} catch (Exception e) {
							logger.catching(e);
						}
					}	0				
				});

			}*/
			
		
	
	}

	private Map<String, Set<Entity>> organizeEnts(Set<Entity> builtEnts) {
		Map<String, Set<Entity>> map = new HashMap<String,Set<Entity>>();
		
		for(Entity entity: builtEnts) {
			String className = entity.getClass().getSimpleName();
			
			if(map.containsKey(className)) {
				Set<Entity> entities = map.get(className);
				
				entities.add(entity);
				
				map.put(className, entities);
			} else {
				Set<Entity> entities = new HashSet<Entity>();
				
				entities.add(entity);
				
				map.put(className, entities);
			}
			
		}
		
		return map;
	}

	private Map<String, Integer> createIndex(List<ObjectMapping> list) {
		Map<String, Integer> index = new HashMap<String,Integer>();
		
		for(int x = 0 ; x < list.size() ; x++) {
			
			index.put(list.get(x).sourceId, new Integer(x));
		}
		return index;
	}

}
