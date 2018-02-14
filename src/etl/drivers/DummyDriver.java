package etl.drivers;

import etl.job.jobtype.JobType;

public abstract class DummyDriver {

	private static final String JOB_TYPE = "DemoJob";
	
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
