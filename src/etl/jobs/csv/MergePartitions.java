package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptCounts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.I2B2Secure;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.Job;
import etl.utils.Utils;

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

