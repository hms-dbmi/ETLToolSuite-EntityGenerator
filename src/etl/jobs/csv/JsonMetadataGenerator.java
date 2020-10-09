package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.jobs.Job;
import etl.jobs.jobproperties.JobProperties;
import etl.metadata.Metadata;
import etl.metadata.MetadataFactory;

/**
 * This class will generate the Json Metadata 
 * Will generate metadata for each study in the ManagedInput file.
 * 
 * 
 * @author Tom DeSain
 *
 */
public class JsonMetadataGenerator extends Job {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 615713376679476882L;
	
	private static String WRITE_FILE_NAME = "metadata.json";
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}	

		try {
			execute();
		} catch (JsonProcessingException e) {
			
			
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	private static void execute() throws JsonProcessingException, IOException {
		/// read managed input file
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		
		List<ManagedInput> managedInputs = ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);

		Metadata etlm = MetadataFactory.buildMetadata(METADATA_TYPE,managedInputs);

		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + WRITE_FILE_NAME) , StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			buffer.write(ow.writeValueAsString(etlm));
			
		}
		
	}
	
/*
	private static Map<String, String> getConsentGroups(String[] studyAccession) {
		// TODO Auto-generated method stub
		return null;
	}

	// this is for json meta data to find 
	private static Map<String, String> buildConsentLookup(String studyIdentifier) throws IOException {
		
		Map<String, String> consentLookup = new HashMap<String,String>();
		boolean dataDictExists = false;
		boolean subjectMultiExists = false;
		
		File dataDir = new File(DATA_DIR);
		
		if(dataDir.isDirectory()) {
			
			String[] fileNames = dataDir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith(studyIdentifier) && name.toLowerCase().contains("subject.multi") && name.toLowerCase().endsWith(".txt")) {
						return true;
					} else if(name.startsWith(studyIdentifier) && name.toLowerCase().contains("subject.data_dict") && name.toLowerCase().endsWith(".xml")) {
						return true;
					} else {
						return false;
					}
				}
			});
			
			if(fileNames.length < 2) {
				
				System.err.println("Expecting a subject.multi and subject.data_dict for the" + studyIdentifier + " aborting!");
				
			}
			
			
			
		} else {
			throw new IOException("parameter DATA_DIR = " + DATA_DIR + " is not a directory!", new Throwable().fillInStackTrace() );
		}
		
		return consentLookup;
	}
	*/
	private static void setLocalVariables(String[] args, JobProperties buildProperties) {
		// TODO Auto-generated method stub
		
	}



}
