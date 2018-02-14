package etl.mapping;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

public abstract class Mapping {
	private enum VALID_TYPES{ CsvToI2b2TMMapping, PatientMapping };
	
	private static String MAPPING_TYPE_PACKAGE = "etl.mapping.";

	
	public abstract Map<String,List<Mapping>> processMapping(Object ... args);
	
	public abstract void buildMapping(String[] arr) throws IOException;
	
	public Mapping(){};
	
	public Mapping(String str) throws Exception{
		
		if(isValidType(str)){
			
		} else {
		
			throw new Exception();
		
		};
	}
	@SuppressWarnings("finally")
	public static Mapping initMappingType(String jobType){
		Mapping newInstance = null;
		try {		
			
			if(isValidType(jobType)){
			
				Class<?> resourceInterfaceClass = Class.forName(MAPPING_TYPE_PACKAGE + jobType);
	
				newInstance =  (Mapping) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(jobType);
				
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
		
	private static boolean isValidType(String str){
		
		for(VALID_TYPES v: VALID_TYPES.values()){
		
			if(v.toString().equals(str)) return true;
		
		}
		
		return false;
	}
	
}
