package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map.Entry;
import java.util.TreeMap;
import etl.jobs.Job;

public class MergePartitions extends Job {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			//setLocalVariables(args);
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
		mergeAllConcepts();
		
	}

	private static void mergeAllConcepts() throws IOException {
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_allConcepts.csv"), StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.CREATE)) {
		    
			File processingdir = new File(PROCESSING_FOLDER);
			
			File[] files = processingdir.listFiles(new FilenameFilter() {
				
				//apply a filter
				@Override
				public boolean accept(File dir, String name) {
					boolean result;
					if(name.contains(TRIAL_ID) && name.contains("allConcepts.csv")){
						result=true;
					}
					else{
						result=false;
					}
					return result;
				}
			
			});
			
			TreeMap<Integer,String> orderedfilelist = new TreeMap<>();
			
			for(File f: files) {
								
				orderedfilelist.put(new Integer(f.getName().split("\\_")[0]), f.getAbsolutePath());
				
			}
			
			for(Entry<Integer,String> entry: orderedfilelist.entrySet()) {
				
				try(BufferedReader reader = Files.newBufferedReader(Paths.get(entry.getValue()))) {
					
					String line;
					
					while((line = reader.readLine()) != null) {
						
						buffer.write(line + '\n');
						
					}
					
				}
				
			}
			
		}
	}
	
}

