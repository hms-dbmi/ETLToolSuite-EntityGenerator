package etl.job.jobtype;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.job.jobtype.properties.JobProperties;

public abstract class JobType {
	
	private enum VALID_TYPES{ JsonToI2b2TM, JsonToI2b2TM2New, XmlToI2b2TM, CSVToI2b2TM2New2, DemoJob };
	
	private static final String JOB_TYPE_PACKAGE = "etl.job.jobtype.";
		
	public JobType(String str) throws Exception{
		
		if(isValidType(str)){
			
		} else {
		
			throw new Exception();
		
		};
		
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
