package etl.data.datatype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.job.entity.Mapping;
import etl.job.entity.hpds.AllConcepts;


public abstract class DataType {
	
	
	private enum VALID_TYPES{ TEXT, OMIT, NUMERIC, MODIFIER, OBJECTARRAY, GENETIC }

	private final static String DATA_TYPE_PACKAGE = "etl.data.datatype.i2b2.";
	
	protected static final String ARRAY_PACKAGE = "etl.data.datatype.i2b2.objectarray.";
	
	public static final String OPTIONS_DELIMITER = ";";
	
	public static final String OPTIONS_KV_DELIMITER = ":";
		
	public static String DEFAULT_SOURCESYSTEM_CD = "";
	
	public static String ROOT_NODE = "";
	
	protected static final String C_METADATAXML = "<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>";
		
	public DataType(String dataType){};
	
	public static boolean isValidDataType(String dataType) {
			
		for(VALID_TYPES v: VALID_TYPES.values()){
		
			if(v.toString().equals(dataType)) return true;
		
		}
		
		return false;
		
	}
	@Deprecated
	public DataType generateDataType(Map map, Mapping mapping, List<AllConcepts> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException {
				// verify is a valid datatype
		if(DataType.isValidDataType(mapping.getDataType())){
			
			DataType dt = DataType.initDataType(mapping.getDataType());
			
			//entites.addAll(dt.generateTables(map, mapping, entities, relationalKey, omissionKey));
			return dt;
		}
		return null;
	}	
	
	@SuppressWarnings("finally")
	public static DataType initDataType(String dataType){
		
		DataType newInstance = null;
		
		try {		
			
			if(isValidDataType(dataType)){
			
				Class<?> resourceInterfaceClass = Class.forName(DATA_TYPE_PACKAGE + dataType.substring(0, 1).toUpperCase() + dataType.substring(1).toLowerCase());
	
				newInstance =  (DataType) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(dataType);
				
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
	@Deprecated

	public abstract Set<AllConcepts> generateTables(Map map, Mapping mapping,
			List<AllConcepts> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException, Exception;
	
	
	// Utility method make a generic kv pair from denormalized string.
	public Map<String,String> makeKVPair(String str, String delimiter, String kvdelimiter){
		Map<String, String> map = new HashMap<String, String>();
		for(String str2: str.split(delimiter)){
		
			String[] split = str2.split(kvdelimiter);
			
			if(split.length == 2){
				
				map.put(split[0], split[1]);
			
			} else if(split.length == 1) {
				map.put(split[0], "");
			}
		}
				
		return map;
	
	}
	public static String buildConceptPath(List<String> pathList) {
		String path = "\\";
		for(String s: pathList){ 
			if(s != null && !s.isEmpty()){
				
				if(s.startsWith("\\")){
					
					s = s.substring(1);
					
				} if ( s.endsWith("\\")){
					
					s= s.substring(0, s.lastIndexOf("\\") - 0);
					
				}
				
				path = path + s + "\\";
			}
		}
		if(path.endsWith("\\\\")) {
			path = path.substring(0, path.lastIndexOf("\\") - 1);

		}
		return path;
	}
	public abstract Set<AllConcepts> generateTables(Mapping mapping, List<AllConcepts> entities, List<Object> values,
			List<Object> relationalValue) throws Exception;
	
}
