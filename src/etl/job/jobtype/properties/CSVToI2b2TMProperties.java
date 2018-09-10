package etl.job.jobtype.properties;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;


public class CSVToI2b2TMProperties extends JobProperties {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4659771078919212075L;
	{
	}
	public CSVToI2b2TMProperties(String str) throws Exception {
		super(str);
		
		this.REQUIRED_PROPERTIES = Arrays.asList
				(
					new String[] { 
							
							
					}
				);
		
	}

}
