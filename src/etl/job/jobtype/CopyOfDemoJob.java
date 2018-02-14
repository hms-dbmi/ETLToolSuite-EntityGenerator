package etl.job.jobtype;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.nio.file.OpenOption;

import org.apache.commons.lang.StringUtils;

import utility.directoryutils.DirUtils;
import au.com.bytecode.opencsv.CSVReader;
import etl.data.datasource.CSVDataSource;
import etl.data.datasource.DataSource;
import etl.data.export.ETLFileWriter;
import etl.data.export.entities.Entity;
import etl.mapping.CsvToI2b2TMMapping;
import etl.mapping.Mapping;
import etl.mapping.PatientMapping;

public class CopyOfDemoJob {
	
	private static final String DATA_DIR = "/Users/tom/Documents/Cardia/"; 

	private static final String DESTINATION_DIR = "/Users/tom/Documents/Cardia/"; 
	
	private static final String WRITE_PROCESSING_DIR = "/Users/tom/Documents/Cardia/Processing/";
	
	private static final String WRITE_COMPLETED_DIR = "/Users/tom/Documents/Cardia/Completed/"; 

	private static final String DELIMITER = "\t"; 

	private static final String MAPPING_DIR = "/Users/tom/Documents/Cardia/Mapping/";
	//mappingcardia
	
	private static final String MAPPING_FILE = "/Users/tom/Documents/Cardia/mappingv2.txt";

	private static final String PATIENT_MAPPING_FILE = "/Users/tom/Documents/Cardia/PatientMapping.csv";
	
	private static final String MAPPING_DELIMITER = ",";
	
	private static final List<String> EXPORT_TABLES = new ArrayList<String>(Arrays.asList("PatientDimension", "ObservationFact", "ConceptDimension", "I2B2"));

	
	private static final String CLEAN_COMMENTS = "T";
	
	private final String sourceSystem = "CARDIA";
	
	private final boolean generateLoadScripts = true;
	
	private final String loadDialect = "Oracle";
	
	// these should be internal to the class leave as constant variable
	private static final String EXPORT_PACKAGE = "etl.data.export.i2b2.";
	
	private static final String ENTITY_PACKAGE = "etl.data.export.entities.i2b2.";

	private static final String FILE_NAME = "/Users/tom/Documents/c_fullname.csv";
	
	public static int totalFiles = 0;

	public static Charset encoding = StandardCharsets.UTF_8;

	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, APPEND };

	public static void main(String[] args) {
		try(Stream<String> stream = Files.lines(Paths.get(FILE_NAME))) {
			
			
			List<String> l = new ArrayList<String>();
			
			stream.forEach(str -> {

				
			});
			
			String str = "\\TEST3\\TEST2\\TEST1\\";
			
			recurseString(l,str.substring(0, StringUtils.ordinalIndexOf(str, "\\", StringUtils.countMatches(str, "\\") - 1) + 1));
			
			Path destPath = Paths.get("/Users/tom/Documents/allNames");

			Files.write(destPath, l, encoding, WRITE_OPTIONS );
			
		} catch(Exception e){
			
			e.printStackTrace();
			
		}
	}

	private static void recurseString(List<String> l, String str) {

		int x = StringUtils.countMatches(str, "\\");

		if(x > 1){
			
			if(!l.contains(str)){
			
				l.add(str);
			
			}
			str = str.substring(0, StringUtils.ordinalIndexOf(str, "\\", x - 1) + 1);
					
			recurseString(l, str);		
			
		}
	}

}
