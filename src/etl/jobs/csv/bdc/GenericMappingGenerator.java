package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.opencsv.CSVReader;

import etl.jobs.mappings.Mapping;

public class GenericMappingGenerator extends BDCJob {

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			setClassVariables(args);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		try {
			execute();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(e);
			
		}	
	}

	private static void execute() throws IOException {
		
		//ROOT_NODE = "PETAL Repository of Electronic Data COVID-19 Observational Study (RED CORAL) ( phs002363 )";
		//DATA_DIR = "./TEST/";
		Set<Mapping> mappings = buildMappings();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_" + "mapping.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Mapping mapping:mappings) {
				writer.write(mapping.toCSV() + '\n');
				
			}
		}
		
	}

	private static Set<Mapping> buildMappings() throws IOException {
		Set<Mapping> mappings = new TreeSet<Mapping>(new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				return o1.getRootNode().compareTo(o2.getRootNode());
			}
		});
		
		// read data dir
		File dataDir = new File(DATA_DIR);
		// Map of rootNode and trial id
		Map<String,String> rootnodes = new TreeMap<>();
		
		if(dataDir.isDirectory()) {
			
			for(File f: dataDir.listFiles()) {
				
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + f.getName()))) {
					
					CSVReader reader = new CSVReader(buffer);
					
					String[] headers = reader.readNext();
					int x = 0;
					
					System.out.println("Processing mappings for: " + f.getName());
					
					if(headers == null) {
						System.out.println(f.getName() + " has an issue with it's headers.");
						continue;
					}
					
					for(String col: headers) {
						
						Mapping mapping = new Mapping();
						
						mapping.setKey(f.getName() + ":" + x);
						mapping.setRootNode(PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + f.getName() + col + PATH_SEPARATOR);
						mapping.setSupPath("");
						mapping.setDataType("TEXT");
						
						mappings.add(mapping);
						
						x++;
						
					}
					
				}
				
			}
			
		}
		return mappings;
	}

	private static void setClassVariables(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
