package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;

import etl.jobs.csv.bdc.BDCJob;

public class PurgePatients extends BDCJob {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		Set<String> validPatientNums = getPatienNums();
		
		purgePatients(validPatientNums);
		
	}

	private static void purgePatients(Set<String> validPatientNums) throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "allConcepts2.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)){
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "allConcepts.csv"))) {
				String line;
				
				while((line = buffer.readLine()) != null) {
					String patNum = line.split(",")[0].replaceAll("\"", "");
					
					if(validPatientNums.contains(patNum)) {
						writer.write(line + '\n');
					}
					
				}
			}
		}
		
	}

	private static Set<String> getPatienNums() throws IOException {
		File dir = new File(DATA_DIR);
		
		File[] files = dir.listFiles();
		Set<String> patnums = new TreeSet<String>();
	
		for(File f: files) {
			if(f.getName().endsWith("PatientMapping.v2.csv")) {
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + f.getName()))) {
					CSVReader reader = new CSVReader(buffer);
					
					String[] line;
					
					while((line = reader.readNext()) != null ) {
						if(line.length >= 3) {
							patnums.add(line[2]);
						}
					}
				}
			};
		}
		return patnums;
	}

}
