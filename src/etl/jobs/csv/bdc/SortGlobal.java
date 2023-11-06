package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;

import etl.job.entity.hpds.AllConcepts;

public class SortGlobal extends BDCJob {

	public static void main(String[] args) {
		try {

			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}	
		
		try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			
		}
	}

	private static void execute() throws IOException {
		Set<AllConcepts> acs = new TreeSet<>( new Comparator<AllConcepts>() {

			@Override
			public int compare(AllConcepts o1, AllConcepts o2) {
				if(o1 == null || o2 == null) return -1;
				
				int conceptpath = o1.getConceptPath().compareTo(o2.getConceptPath());
				
				if(conceptpath != 0) {
					return conceptpath;
				}
				
				int patientNum = o1.getPatientNum().compareTo(o2.getPatientNum());
				
				if(patientNum != 0) {
					return patientNum;
				}
				
				int tvalChar = o1.getTvalChar().compareTo(o2.getTvalChar());
				
				if(tvalChar != 0) {
					return tvalChar;
				}
				
				return(o1.getNvalNum().compareTo(o2.getNvalNum()));
				
										
			}
			
		} );
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "GLOBAL_allConcepts.csv"))) {
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			while((line = reader.readNext())!=null) {
				acs.add(new AllConcepts(line));
			}
			
		}
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "ORCHID_Globalvars.csv"))) {
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			while((line = reader.readNext())!=null) {
				acs.add(new AllConcepts(line));
			}
			
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "GLOBAL_Joined.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			for(AllConcepts ac: acs) {
				writer.write(ac.toCSV());
			}
		}
	}

}
