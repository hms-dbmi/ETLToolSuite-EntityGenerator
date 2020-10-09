package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

import etl.job.entity.Mapping;
import etl.jobs.Job;

public class RootNodeConcept extends Job {

	private static String ALL_CONCEPTS_FILE = "GLOBAL_allConcepts.csv";

	public static void main(String[] args) {
		try {

			setVariables(args, buildProperties(args));
							
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
			System.err.println(e);
		}
		
		
		try {
			
			execute();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void execute() throws IOException {
		// get mappings 
		List<Mapping> mappings = Mapping.generateMappingListForHPDS(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		// get patient nums
		List<String> patientnums = getAllPatientNums();
		
		writeMapping(patientnums, mappings.get(0));
		
		write(patientnums, mappings.get(0));
	}

	private static void writeMapping(List<String> patientnums, Mapping mapping) throws IOException {
		
		String cp = "µ_studiesµ" + mapping.getRootNode().substring(1).replaceAll("µ.*", "") + "µ";

		Mapping m = new Mapping();
		
		m.setKey(TRIAL_ID + "rootnode" + ":1");
		m.setRootNode(cp);
		m.setDataType("TEXT");
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			buffer.write(m.toCSV() + '\n');
		}
		
	}

	private static void write(List<String> patientnums, Mapping mapping) throws IOException {
		
		String cp = "µ_studiesµ" + mapping.getRootNode().substring(1).replaceAll("µ.*", "");
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID.toLowerCase() + "rootnode.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			
			for(String patNum: patientnums) {
				
				String[] linetowrite = new String[2];
				
				linetowrite[0] = patNum;
				linetowrite[1] = "TRUE";
				
				buffer.write(toCsv(linetowrite));
			}
		}
		
	}

	private static List<String> getAllPatientNums() throws IOException {
		
		List<String> patientNums = new ArrayList<String>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + TRIAL_ID.toUpperCase() + "_PatientMapping.csv"))) {
			
			CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
			
				if(line.length != 3) continue;
				
				patientNums.add(line[2]);
				
			}
			
		}
		
		return patientNums;
		
	}

}
