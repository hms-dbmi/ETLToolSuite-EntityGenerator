package etl.job.jobtype.properties;

import java.util.Arrays;

public class JsonToI2b2TMProperties extends JobProperties {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4659771078919212075L;
	{
	}
	public JsonToI2b2TMProperties(String str) throws Exception {
		super(str);
		
		this.REQUIRED_PROPERTIES = Arrays.asList
				(
					new String[] { 
							
							"filename", 
							"writedestination", 
							"mappingfile"
							
					}
				);
		
	}
}
