package etl.data.export.entities.i2b2.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ObjectMapping;

public class ColumnSequencer {
	public List<String> entityNames = new ArrayList<String>();
	public String entityColumn;
	public String columnType;
	public String mappedName;
	public int startSequence;
	public int interval = 1;
	public int currSequence;
	public ArrayList<String> values = new ArrayList<String>();
	
	public Set<ObjectMapping> generateSeqeunce(Set<Entity> builtEnts) throws Exception{
		 //List<String> seq = new ArrayList<String>();
		Set<ObjectMapping> objectMappings = new HashSet<ObjectMapping>();
		
		for(Entity entity: builtEnts) {
			if(entityNames.indexOf(entity.getClass().getSimpleName()) != -1) {
				
				Field field = entity.getClass().getDeclaredField(this.entityColumn);	
				field.setAccessible(true);
				Object sourceId = field.get(entity);
				
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
		return objectMappings;
	}
	
	public int findSeqId(String searchfield) {
		
		int index;
		//System.out.println(this.values.indexOf(searchfield));
		if(this.values.indexOf(searchfield) < 0) {

			index = this.currSequence;
			this.currSequence = this.currSequence + this.interval;
			
			this.values.add(searchfield);

		} else {
			index = this.values.indexOf(searchfield) + startSequence;
		}
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

	public Integer nextVal() {
		this.currSequence = this.currSequence + this.interval;
		return new Integer(this.currSequence);
		
	}
	

}
