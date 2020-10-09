package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.opencsv.CSVReader;

import etl.jobs.Job;

public class AddStartDateFix extends Job {

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
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "allConcepts2.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			try(BufferedReader wbuffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "allConcepts.csv"))) {
				
				CSVReader reader = new CSVReader(wbuffer);
				
				String line[];
				
				buffer.write("patientnum,conceptpath,nval,tval,startdate\n");
				
				while((line = reader.readNext()) != null) {
					if(line.length == 4) {
						
						String[] newline = new String[5];
						
						newline[0] = line[0];
						newline[1] = line[1];
						newline[2] = line[2];
						newline[3] = line[3];
						newline[4] = "0";
						
						buffer.write(toCsv(newline));
						
					} else if (line.length == 5){
						
						buffer.write(toCsv(line));
						
					}
					
				}
				
			}
			
		}
		
	}
	

}
