package etl.drivers;


import com.csvreader.CsvReader;

import etl.job.jobtype.JobType;

public class CSVDriver {
	private static final String JOB_TYPE = "CsvToI2b2TM";
	
	public static void main(String[] args) {
		try {
			
			JobType job = JobType.initJobType(JOB_TYPE);
			
			long startTime = System.currentTimeMillis();		

			job.runJob();
			
			long stopTime = System.currentTimeMillis();
			
		    long elapsedTime = stopTime - startTime;
		    
		     System.out.println("Job runtime(ms): " + elapsedTime);	
		} catch (Exception e) {

			e.printStackTrace();
		
		}
	}

}
