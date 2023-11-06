package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;

public class addSourceIds extends BDCJob {

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
			
		}
		
	}

	private static void execute() throws IOException {
		
		List<char[]> toWrite = new ArrayList<>();

		List<String[]> jinling = new ArrayList<String[]>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "jinlingextract.csv"))) {
			
			CSVReader reader = new CSVReader(buffer, ',','"','µ');
			
			jinling = reader.readAll();
			
		}
		
		List<String[]> multi = new ArrayList<String[]>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "phs000007.v30.pht000182.v13.p11.Framingham_Subject.MULTI.txt"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			multi = reader.readAll();
			
		}
		
		List<String[]> appended = new ArrayList<String[]>();
		int x = 0;
		for(String[] rec: jinling) {
			
			String[] jl_Join = rec[19].split("\\_");
			
			String ssji = findSourceId(jl_Join[1], multi);
				
			rec = Arrays.copyOf(rec, rec.length + 1);
			
			rec[rec.length - 1] = (x == 0) ? "subject_id": jl_Join[1];
			
			rec = Arrays.copyOf(rec, rec.length + 1);
			
			rec[rec.length - 1] = (x == 0) ? "source_subject_id": ssji;
			x++;
			toWrite.add(toCsv(rec));
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Appended_Subject_SourceId.csv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(char[] rec: toWrite) {
				
				writer.write(rec);
			}
		}
	}

	private static String findSourceId(String jl_Join, List<String[]> multi) {
		
		for(String[] m: multi) {
			
			if(m[1].equals(jl_Join)) return m[6];
			
		}
		
		return null;
	}

}
