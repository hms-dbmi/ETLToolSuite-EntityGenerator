package etl.data.datatype.i2b2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datatype.DataType;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.mappings.Mapping;



public class Omit extends DataType {

	public Omit(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Set<AllConcepts> generateTables(Map map, Mapping mapping, List<AllConcepts> entities,
			String relationalKey, String omissionKey) {
				return null;
		
		
	}

	@Override
	public Set<AllConcepts> generateTables(Mapping mapping, List<AllConcepts> entities, List<Object> values,
			List<Object> relationalValue) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


}
