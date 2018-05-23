package etl.data.datatype.i2b2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datatype.DataType;
import etl.data.export.entities.Entity;
import etl.job.jsontoi2b2tm.entity.Mapping;
import etl.mapping.CsvToI2b2TMMapping;

public class Omit extends DataType {

	public Omit(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Set<Entity> generateTables(Map map, Mapping mapping, List<Entity> entities,
			String relationalKey, String omissionKey) {
				return null;
		
		
	}

	@Override
	public Set<Entity> generateTables(String[] data,
			CsvToI2b2TMMapping mapping, List<Entity> entities) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Set<Entity> generateTables(Mapping mapping, List<Entity> entities, List<Object> values,
			List<Object> relationalValue) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


}
