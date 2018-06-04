package etl.data.datasource;

import java.io.FileNotFoundException;

import etl.job.jobtype.JobType;

public abstract class DataSource {
	private enum VALID_TYPES{ FILE, JSONFILE, JSON, XMLFILE, CSVFILE, CSVDataSource, CsvDataSource, JSONDataSource2 };
	
	private static final String DATASOURCE_TYPE_PACKAGE = "etl.data.datasource.";
	
	public DataSource(String str) throws Exception{
		
		if(isValidType(str)){
			
		} else {
		
			throw new Exception();
		
		};
	}
	@SuppressWarnings("finally")
	public static DataSource initDataSourceType(String jobType){
		DataSource newInstance = null;
		try {		
			
			if(isValidType(jobType)){
			
				Class<?> resourceInterfaceClass = Class.forName(DATASOURCE_TYPE_PACKAGE + jobType);
	
				newInstance =  (DataSource) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(jobType);
				
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
	
	public abstract Object processData(String ... arguments) throws FileNotFoundException;
}
