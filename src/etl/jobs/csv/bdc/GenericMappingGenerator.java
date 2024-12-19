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
	protected static boolean HASDATATABLES = false;
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
		if (mappings.isEmpty()){
			System.err.println("Mappings for " + TRIAL_ID + " empty - verify your files and try again.");
			System.exit(255);
		}
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
				return o1.getKey().compareTo(o2.getKey());
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
					
					
					if(f.getName().startsWith("._")){
						continue;
					}
					else{
						System.out.println("Processing mappings for: " + f.getName() + " with datatable format set to " + HASDATATABLES);
					}

					if(headers == null) {
						System.err.println(f.getName() + " has an issue with it's headers.");
						continue;
					}
					
					for(String col: headers) {
						
						Mapping mapping = new Mapping();
						
						mapping.setKey(f.getName() + ":" + x);
						if (HASDATATABLES){
							String tablename= f.getName().substring(0, f.getName().lastIndexOf('.')).replace(".", PATH_SEPARATOR);
							mapping.setRootNode(
								//if datatables enabled, sets the paths to include the filename, with . replaced by path separator,  as an identifier
									PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + tablename + PATH_SEPARATOR + col + PATH_SEPARATOR);
						}
						
						else{
							mapping.setRootNode(PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + col + PATH_SEPARATOR);
						}
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
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-hasDatatables")) {
				HASDATATABLES= true;
			}
		}
		
	}

}
