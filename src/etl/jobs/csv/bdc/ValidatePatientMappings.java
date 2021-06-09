package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import com.opencsv.CSVReader;

public class ValidatePatientMappings extends BDCJob {

	public static void main(String[] args) {
		try {
			
			setVariables(args, buildProperties(args));
						
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}
		
		try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			e.printStackTrace();
		} 

	}

	private static void execute() throws IOException {
		File dataDir = new File(DATA_DIR);
		Set<String> patientNums = new HashSet<>();
		if(dataDir.isDirectory()) {
			
			File[] files = dataDir.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.endsWith("PatientMapping.v2.csv")) return true;
					return false;
				}
			});
			
			for(File f: files) {
				System.out.println("Validating " + f.getName());
				try(BufferedReader buffer = Files.newBufferedReader(f.toPath())) {
					
					CSVReader reader = new CSVReader(buffer);
					String[] line;
					while((line = reader.readNext()) != null) {
						
						if(patientNums.contains(line[2])) {
							System.err.println(f.getName() + " contains patient collision " + line[2]);
						} else {
							patientNums.add(line[2]);
						}
						
						
						
					}
					
				}
				
			}
		}
		
	}

}
