package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import etl.metadata.bdc.GenericBDCMetadata;

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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	private static void execute() throws JsonProcessingException, IOException, Exception {
		/// read managed input file
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		
		List<ManagedInput> managedInputs = ManagedInputFactory.readManagedInput(JOB_TYPE,MANAGED_INPUT);
		
		// Call a generic builder for any studies that do not have a set data model such as dbgap studies.
		// Generic metadata can only create one study at a time that has one consent group 
		Metadata etlm = MetadataFactory.buildMetadata(METADATA_TYPE,managedInputs,new File(DATA_DIR + "metadata.json"));;
		
		/*
		if(!METADATA_TYPE.equals("BDC_GENERIC")) {
			etlm = MetadataFactory.buildMetadata(METADATA_TYPE,managedInputs,new File(DATA_DIR + "metadata.json"));
		} else {
			etlm = MetadataFactory.buildMetadata(METADATA_TYPE,managedInputs,new File(DATA_DIR + "metadata.json"));
		}*/
		if(etlm != null) {
			try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + WRITE_FILE_NAME) , StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				
				buffer.write(ow.writeValueAsString(etlm));
				
			}
		} else {
			System.err.println("Error building metadata!");
			throw new Exception();
		}
	}
	
	private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
		for(String arg: args) {	
			if(arg.equalsIgnoreCase( "-consentgroupfullname" )){
				GenericBDCMetadata.GENERIC_CONSENT_GROUP_FULL_NAME = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-consentgroupabvname" )){
				GenericBDCMetadata.GENERIC_CONSENT_GROUP_ABV_NAME = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-consentgroupcode" )){
				GenericBDCMetadata.GENERIC_CONSENT_CODE = checkPassedArgs(arg, args);
			}
			if(arg.equalsIgnoreCase( "-studyaccession" )){
				GenericBDCMetadata.STUDY_ACCESSION = checkPassedArgs(arg, args);
			} 
		}
	}
	
}
