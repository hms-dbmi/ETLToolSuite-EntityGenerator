package etl.jobs.csv;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.jobs.Job;

public class MetadataUpdater extends Job {

	private static String WRITE_FILE_NAME = "metadata.json";
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			
			
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

	private static void execute() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		
		List<ManagedInput> managedInputs = ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);		
	}

}
