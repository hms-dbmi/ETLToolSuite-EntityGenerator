package etl.drivers;


import com.csvreader.CsvReader;

import etl.job.jobtype.JobType;

public class TestDriver {
	private static final String JOB_TYPE = "CSVNew";
	
	//private static final String JOB_TYPE = "XmlToI2b2TM";
	
	public static void main(String[] args) {
		try {
		
			JobType job = JobType.initJobType(JOB_TYPE);
					
			job.runJob();
			
		} catch (Exception e) {

			e.printStackTrace();
		
		}
	}

}
