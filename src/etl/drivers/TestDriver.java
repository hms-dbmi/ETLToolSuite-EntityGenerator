package etl.drivers;

import etl.job.jobtype.JobType;
import etl.job.jobtype.properties.JobProperties;

public class TestDriver {
	
	private static String JOB_TYPE = "";
	
	private static String PROPERTIES_FILE = "resources/ETLDefault.config";
	
	private static JobProperties JOB_PROPERTIES;
	
	public static void main(String[] args) {
		try {
			
			processArguments(args);
			
			JobType job = JobType.initJobType( JOB_TYPE );
			
			JOB_PROPERTIES = JobProperties.initJobPropType( JOB_TYPE );

			JOB_PROPERTIES = JOB_PROPERTIES.buildProperties( PROPERTIES_FILE );
						
			job.runJob( JOB_PROPERTIES );
			
		} catch (Exception e) {

			e.printStackTrace();
		
		}
	}

	private static void processArguments(String[] args) throws Exception {
		
		for(String arg: args) {
			
			if(arg.equalsIgnoreCase( "-jobtype" )){
				
				JOB_TYPE = checkPassedArgs(arg, args);
				
			} else if (arg.equalsIgnoreCase( "-propertiesfile" )) {

				PROPERTIES_FILE = checkPassedArgs(arg, args);
				
			}
							
		}
		
	}

	// checks passed arguments and sends back value for that argument
	public static String checkPassedArgs(String arg, String[] args) throws Exception {
		
		int argcount = 0;
		
		String argv = new String();
		
		for(String thisarg: args) {
			
			if(thisarg.equals(arg)) {
				
				break;
				
			} else {
				
				argcount++;
				
			}
		}
		
		if(args.length > argcount) {
			
			argv = args[argcount + 1];
			
		} else {
			
			throw new Exception("Error in argument: " + arg );
			
		}
		return argv;

	}
}
