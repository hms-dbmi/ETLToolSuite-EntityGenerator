package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;

/**
 * 
 * This class will merge Global All Concepts
 * 
 *
 */
public class MergeGlobalAllConcepts extends BDCJob {

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
		Set<String[]> allConcepts = mergeGlobalAllConcepts();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "GLOBAL_allConcepts.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			for(String[] arr: allConcepts) {
				writer.write(toCsv(arr));
			}
			
		}
		
	}

	private static Set<String[]> mergeGlobalAllConcepts() throws IOException {
		
		Set<String[]> set = new TreeSet<String[]>( new Comparator<String[]>() {
			
			@Override
			public int compare(String[] o1, String[] o2) {
				
				int concept = o1[1].compareTo(o2[1]);
				int patient = new Integer(o1[0]).compareTo(new Integer(o2[0]));
				int tval = o1[2].compareTo(o2[2]);
				int nval = o1[3].compareTo(o2[3]);
				if(concept != 0) {
					return concept;
				}
				if(patient != 0) {
					return patient;
				}
				if(tval != 0 ) {
					return tval;
				}
				return nval;
			}
			
		});
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			set.addAll(reader.readAll());
		}
		
		File dataDir = new File(DATA_DIR);
		
		if(dataDir.isDirectory()) {
			File[] files = dataDir.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					
					return name.toLowerCase().contains("globalvars.csv");
					
				}
				
			});
			for(File f: files) {
				try(BufferedReader buffer = Files.newBufferedReader(f.toPath())) {
					
					CSVReader reader = new CSVReader(buffer);
					set.addAll(reader.readAll());
				}
			}
		}
		
		return set;
	}

}
