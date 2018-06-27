package etl.job.jobtype;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;
import etl.job.jobtype.properties.JobProperties;

public abstract class JobType {
	protected final Logger logger = LogManager.getLogger(getClass());

	private enum VALID_TYPES{ DataPreview, JsonToI2b2TM, JsonToI2b2TM2New, XmlToI2b2TM, CSVToI2b2TM2New2, DemoJob };
	
	private static final String JOB_TYPE_PACKAGE = "etl.job.jobtype.";
		
	public JobType(String str) throws Exception{
		
		if(isValidType(str)){
			
		} else {
		
			throw new Exception();
		
		};
		
	}
	protected static Map<String, Map<String, String>> generateDataDict(String dictFile) throws IOException {
		Map<String, Map<String, String>> returnmap = new HashMap<String, Map<String, String>>();
		
		File fileToRead = new File( dictFile);
		
		au.com.bytecode.opencsv.CSVReader reader = new CSVReader(new FileReader((File) fileToRead), ',', '"','\0', 1);
		
		for(String[] rec: reader.readAll()) {
			if(returnmap.containsKey(rec[0])) {
				Map<String,String> dictMap = returnmap.get(rec[0]);
				dictMap.put(rec[1], rec[2]);
				returnmap.put(rec[0], dictMap);
			} else {
				Map<String,String> dictMap = new HashMap<String,String>();
				dictMap.put(rec[1], rec[2]);
				returnmap.put(rec[0], dictMap);
			}
		};
		
		return returnmap;
	}
	@SuppressWarnings("finally")
	public static JobType initJobType(String jobType){
		JobType newInstance = null;
		try {		
			
			if(isValidType(jobType)){
			
				Class<?> resourceInterfaceClass = Class.forName(JOB_TYPE_PACKAGE + jobType);
	
				newInstance =  (JobType) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(jobType);
				
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
	
	public abstract void runJob(JobProperties jobProperties);
	
	public abstract void setVariables(JobProperties jobProperties) throws ClassNotFoundException;
	
	public Map<String,String> makeKVPair(String str, String delimiter, String kvdelimiter){
		Map<String, String> map = new HashMap<String, String>();
		for(String str2: str.split(delimiter)){
		
			String[] split = str2.split(kvdelimiter);
			
			if(split.length == 2){
				
				map.put(split[0], split[1]);
			
			}			
		}
				
		return map;
	
	}
}
