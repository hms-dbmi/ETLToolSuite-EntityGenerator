package etl.drivers;


import com.csvreader.CsvReader;

import etl.job.jobtype.JobType;

public class CCDADriver {
	
	private static final String JOB_TYPE = "JsonToI2b2TM";
	
	public static void main(String[] args) {
		try {
		
			JobType job = JobType.initJobType(JOB_TYPE);
			
			job.runJob();
			
		} catch (Exception e) {

			e.printStackTrace();
		
		}
	}

}
