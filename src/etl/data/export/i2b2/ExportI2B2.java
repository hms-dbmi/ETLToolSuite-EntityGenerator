package etl.data.export.i2b2;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import etl.data.datatype.DataType;
import etl.data.export.Export;
import etl.data.export.ExportInterface;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;


public class ExportI2B2 extends Export implements ExportInterface{
	
	private enum VALID_TABLES{ OBSERVATION_FACT, PATIENT_DIMENSION, CONCEPT_DIMENSION, I2B2, TABLE_ACCESS, CONCEPT_COUNTS, NODE_METADATA, MODIFIER_DIMENSION }
	
	public ExportI2B2(String exportType) {
		super(exportType);
		// TODO Auto-generated constructor stub
	}

	@SuppressWarnings("unused")
	private static boolean isValidTable(String str){
		
		for(VALID_TABLES v: VALID_TABLES.values()){
			
			if(v.toString().equals(str)) {
				
				return true;
				
			}
		
		}
		
		return false;
	}
	
	// use to generate specific outputs based on dataType given.  Each DataType will determine how the tables are generated.

	
	// use if you want to create another generic export
	@Override
	public void generateExport() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void generateTables(Map map, Mapping mapping, List<Entity> entities,
			String relationalKey, String omissionKey)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		for(Entity entity: entities)
			try {
				{
					/*if( entity instanceof ObservationFact ) {
						
					} else if( entity instanceof ConceptDimension ) {
						
					} else if( entity instanceof I2B2 ) {
						
					} */
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}

	
	
}
