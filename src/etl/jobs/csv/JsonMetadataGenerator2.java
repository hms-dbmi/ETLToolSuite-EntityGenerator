package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import etl.metadata.bdc.BDCMetadata;
import etl.metadata.bdc.BDCMetadataElements;

/**
 * This class will generate the Json Metadata 
 * Will generate metadata for each study in the ManagedInput file.
 * 
 * 
 * @author Tom DeSain
 *
 */
public class JsonMetadataGenerator2 extends Job {
	
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
		
		BDCMetadata etlm = (BDCMetadata) MetadataFactory.buildMetadata(METADATA_TYPE,managedInputs,new File(DATA_DIR + "metadata.json"));
		
		Map<String,String> patient_counts = buildPatientMap();
		
		Map<String,String> genomic_counts = buildGenomicMap();

		
		Map<String,String> concept_counts = buildPatientMap();
		
		updateCounts(patient_counts,concept_counts,genomic_counts,etlm);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + WRITE_FILE_NAME) , StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			buffer.write(ow.writeValueAsString(etlm));
			
		}
		
	}

	private static void updateCounts(Map<String, String> patient_counts, Map<String, String> concept_counts,
			Map<String, String> genomic_counts, BDCMetadata etlm) {
		for(BDCMetadataElements element : etlm.bio_data_catalyst) {
			
			String phs = element.study_identifier + "." + element.consent_group_code;
			
			if(patient_counts.containsKey(phs)) element.clinical_sample_size = new Integer(patient_counts.get(phs));
			else element.clinical_sample_size = 0;

			if(genomic_counts.containsKey(phs)) element.genetic_sample_size = new Integer(genomic_counts.get(phs));
			else element.genetic_sample_size = 0;
			
			if(concept_counts.containsKey(phs)) element.clinical_variable_count = new Integer(concept_counts.get(phs));
			else element.clinical_variable_count = 0;
						
			
		}
		
	}

	private static Map<String, String> buildGenomicMap() throws IOException {
		Map<String,String> map = new HashMap<String, String>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "genomic_samples"))) {
			
			String line;
			while((line = buffer.readLine())!= null) {
				String[] arr = line.split("\t");
				
				map.put(arr[0], arr[1]);
			}
			
		}
		return map;
	}

	private static Map<String, String> buildPatientMap() throws IOException {
		Map<String,String> map = new HashMap<String, String>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "patient_counts"))) {
			
			String line;
			while((line = buffer.readLine())!= null) {
				String[] arr = line.split("\t");
				
				map.put(arr[0], arr[1]);
			}
			
		}
		return map;
	}

	private static Map<String, String> buildConceptMap() throws IOException {
		Map<String,String> map = new HashMap<String, String>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "concept_counts"))) {
			
			String line;
			while((line = buffer.readLine())!= null) {
				String[] arr = line.split("\t");
				
				map.put(arr[0], arr[1]);
			}
			
		}
		return map;
	}
	
	private static void setLocalVariables(String[] args, JobProperties buildProperties) {
		// TODO Auto-generated method stub
		
	}

}
