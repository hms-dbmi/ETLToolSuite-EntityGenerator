package src.deprecated;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.opencsv.CSVReader;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

public class DataEvaluation2 extends Job {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (InstantiationException e) {
			System.err.println(e);

		} catch (IllegalAccessException e) {
			System.err.println(e);

		} catch (IOException e) {
			System.err.println(e);

		}
	}

	private static void execute() throws InstantiationException, IllegalAccessException, IOException {
		List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		List<PatientMapping> patientMappingFile = !PATIENT_MAPPING_FILE.isEmpty() ? PatientMapping.class.newInstance().generateMappingList(PATIENT_MAPPING_FILE, MAPPING_DELIMITER): new ArrayList<PatientMapping>();

		Collections.sort(mappingFile, new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				return o1.getRootNode().compareTo(o2.getRootNode());
				
			}
		});//(mappingFile);
		String currentRootNode = "";
		
		List<String> values = new ArrayList<String>();
		
		for(Mapping mapping: mappingFile) {
			
			if(currentRootNode.equals(mapping.getRootNode())) {
				
				findNodesValues(mapping, values);
				
			} else {
				
				currentRootNode = mapping.getRootNode();
				
				values = new ArrayList<String>();
				
			}
						
		}
		
		
	}

	private static void findNodesValues(Mapping mapping, List<String> values) throws IOException {
		if(Files.exists(Paths.get(DATA_DIR + mapping.getKey().split(":")[0]))) {
			Path path = Paths.get(DATA_DIR + mapping.getKey().split(":")[0]);
			
			try(BufferedReader buffer = Files.newBufferedReader(path)){
				
				CSVReader reader = new CSVReader(Files.newBufferedReader(path));
				
				if(SKIP_HEADERS) reader.readNext(); 
				
				int colnum = new Integer(mapping.getKey().split(":")[1]);
				
				String[] line;
				
				while((line = reader.readNext()) != null) {
					
					values.add(line[colnum]);
					
				}
				
			}
			
		} else {
			System.err.println(DATA_DIR + mapping.getKey().split(":")[0] + " Does not exist!");
		}
	}

}
