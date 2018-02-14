package etl.data.datasource.loader;

import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datasource.loader.loaders.OracleControlFile;


public abstract class Loader {
	
	private enum VALID_TYPES{ Oracle };
	
	private static final String JOB_TYPE_PACKAGE = "etl.data.datasource.loader.loaders.";
	
	public static String DIALECT = "";

	/**
	 * This will generate a load for each entity sent for desired environment
	 * Credientials should be handled by user( or methodology used to execute scripts ex: jenkins ) entry when load is executed
	 * Map returned is structured as: Key = fileName to be generated. List<String> lines of text to put in file
	 * @return
	 */
	public abstract Map<String, OracleControlFile> generateLoadTest(List<String> exportTables);
		
	public Loader(String str) throws Exception{
		
		if(isValidType(str)){
			
		} else {
		
			throw new Exception();
		
		};
		
	}
	
	@SuppressWarnings("finally")
	public static Loader initLoaderType(String dialect){
		Loader newInstance = null;
		try {		
			
			if(isValidType(dialect)){
			
				Class<?> resourceInterfaceClass = Class.forName(JOB_TYPE_PACKAGE + dialect);
	
				newInstance =  (Loader) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(dialect);
				
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
			if(v.toString().equals(str)) {
				return true;
			}
		
		}
		
		return false;
	}


}
