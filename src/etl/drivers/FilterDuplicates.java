package etl.drivers;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.opencsv.bean.CsvToBean;

import etl.job.entity.Mapping;
/**
 * Scans the mapping file(s) to look for duplicate paths.
 * If found look at variables for those mappings to ensure no duplicate 
 * concepts will be created.  If duplicates are found throw exception 
 * detailing why this is invalid.  They would need another grain of complexity.
 * probably separate encounters.
 * 
 * @author Tom
 *
 */
public class FilterDuplicates {
	private static final List<LinkOption> options = null;
	
	private static boolean SKIP_HEADERS = true;

	private static String WRITE_DIR = "./completed/";
	
	private static String MAPPING_FILE = "./mappings/mapping.csv";

	private static boolean MAPPING_SKIP_HEADER = true;

	private static char MAPPING_DELIMITER = ',';

	private static char MAPPING_QUOTED_STRING = '"';

	private static final char DATA_SEPARATOR = ',';

	private static final char DATA_QUOTED_STRING = '"';
	
	private static String DATA_DIR = "./data/";

	private static String TRIAL_ID = "DEFAULT";
	
	private static boolean IS_PARTITION = false;
	
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e);		}
	}
	
	private static void execute() throws IOException {
		if(!IS_PARTITION) {
			doNonPartition();
		}
		
	}

	private static void doNonPartition() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(MAPPING_FILE))){
			List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
			
			List<String> mappingsDupCheck = new ArrayList<String>();
			
			List<String> dupes = new ArrayList<String>();
			
			List<String> conceptRoots = mappings.stream().collect(Collectors.mapping(Mapping::getRootNode, Collectors.toList()));
			
			conceptRoots.stream().forEach(concept -> {
				if(mappingsDupCheck.contains(concept)) {
					dupes.add(concept);
				} else {
					mappingsDupCheck.add(concept);
				}
			});
			
			dupes.stream().forEach(dupe ->{
				System.out.println("Duplicates for: " + dupe );
				for(Mapping m: mappings) {
					if(m.getRootNode().equals(dupe)) {
						System.err.println(m);
					}
				}
				
			});
			if(!dupes.isEmpty()) {
				
			}
		}
		
	}

	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-skipheaders")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					SKIP_HEADERS = true;
				} 
			}
			if(arg.equalsIgnoreCase( "-mappingskipheaders" )){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					MAPPING_SKIP_HEADER = true;
				} 
			}
			if(arg.equalsIgnoreCase( "-mappingquotedstring" )){
				String qs = checkPassedArgs(arg, args);
				MAPPING_QUOTED_STRING = qs.charAt(0);
			}
			if(arg.equalsIgnoreCase( "-mappingdelimiter" )){
				String md = checkPassedArgs(arg, args);
				MAPPING_DELIMITER = md.charAt(0);
			}
			if(arg.equalsIgnoreCase( "-mappingfile" )){
				MAPPING_FILE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-writedir" )){
				WRITE_DIR = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-trialid" )){
				TRIAL_ID  = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-ispartition" )){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					IS_PARTITION = true;
				} 
			}
			
		}
	}
	// checks passed arguments and sends back value for that argument
	public static String checkPassedArgs(String arg, String[] args) throws Exception {
		
		int argcount = 0;
		
		String argv = new String();
		
		for(String thisarg: args) {
			
			if(thisarg.equals(arg)) {
				
				break;
				
			} else {
				
				argcount++;
				
			}
		}
		
		if(args.length > argcount) {
			
			argv = args[argcount + 1];
			
		} else {
			
			throw new Exception("Error in argument: " + arg );
			
		}
		return argv;
	}
}
