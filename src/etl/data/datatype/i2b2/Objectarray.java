package etl.data.datatype.i2b2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.Data;

import etl.data.datasource.JSONDataSource;
import etl.data.datatype.DataType;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.mappings.Mapping;



public class Objectarray extends DataType {
	
	private enum VALID_ARRAYS{ UMLS, KEYVALUE, MODIFIER, AARRAY, VALUEARRAY }
	
	public static String ARRAY_FORMAT = "JSON";
	
	public Objectarray(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}
	@Override
	public  Set<AllConcepts> generateTables(Mapping mapping, List<AllConcepts> entities, List<Object> values,
			List<Object> relationalValues) throws Exception{
		
		Map<String, String> options = Mapping.class.newInstance().buildOptions(mapping.getOptions());
		if(!isValidArrayType(options.get("TYPE"))) {
			
			throw new Exception();	
		
		}
		
		Objectarray oa = Objectarray.initDataType(options.get("TYPE"));

		return oa.generateTables(mapping, entities, values, relationalValues);
		
	}
	
	@Deprecated
	public Set<AllConcepts> generateTables(Map map, Mapping mapping,
			List<AllConcepts> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException, Exception {

		Map<String, String> options = Mapping.class.newInstance().buildOptions(mapping.getOptions());
		if(!isValidArrayType(options.get("TYPE"))) {
			
			throw new Exception();	
		
		}
		
		
		Objectarray oa = Objectarray.initDataType(options.get("TYPE"));

		return oa.generateTables(map, mapping, entities, relationalKey, omissionKey);
		
	}

	
	public static boolean isValidArrayType(String arrType) {
		
		for(VALID_ARRAYS v: VALID_ARRAYS.values()){
			
			if(v.toString().equalsIgnoreCase(arrType)) return true;
		
		}
		
		return false;
		
	}

	public static Objectarray initDataType(String dataType){
		
		Objectarray newInstance = null;
		
		try {		
			
			if(isValidArrayType(dataType)){

				Class<?> resourceInterfaceClass = Class.forName(ARRAY_PACKAGE + dataType.substring(0, 1).toUpperCase() + dataType.substring(1).toLowerCase());
	
				newInstance =  (Objectarray) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(dataType);
				
			} else {
				
				throw new Exception();
				
			}
			
		} catch (SecurityException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (InstantiationException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (IllegalAccessException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (ClassNotFoundException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (Exception e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} finally {
			
			return newInstance;
		}
		
	}
	
	protected Set<String> generateKeys(Map map, String key) {
		
		Set<String> set = new HashSet<String>();
		
		for(Object obj: map.keySet()){
		
			if(obj.toString().contains(key)){
			
				set.add(obj.toString());
			
			}
		
		}
		
		return set;
	
	}
	
	protected Set<String> generateKeys(String keys, String key) {
		
		Set<String> set = new HashSet<String>();

		for(Object obj: keys.split("\\|")){
				
			set.add(obj.toString());
		
		}
		
		return set;
	
	}

	public List<Map<String, List<String>>> generateArray(Object object){

		if(object == null || object.toString().equals("")){
		
			return new ArrayList<Map<String, List<String>>>();
		
		}
		
		if(ARRAY_FORMAT.equalsIgnoreCase("JSONFILE")){

			return buildJsonArray(object);
			
		}
		return null;
	}

	private List<Map<String, List<String>>> buildJsonArray(Object string) {

		try {
			
			JSONDataSource jds = new JSONDataSource();

			return jds.processJsonArrayToMap(string.toString());
						
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
}
