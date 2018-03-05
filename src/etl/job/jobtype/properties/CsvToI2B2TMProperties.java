package etl.job.jobtype.properties;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;


public class CsvToI2B2TMProperties extends JobProperties {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4659771078919212075L;
	{
	}
	public CsvToI2B2TMProperties(String str) throws Exception {
		super(str);
		
		this.REQUIRED_PROPERTIES = Arrays.asList
				(
					new String[] { 
							
							"datadir", 
							"processingdir", 
							"completeddir", 
							"mappingfile", 
							"patientmappingfile"
							
					}
				);
		
	}

}
