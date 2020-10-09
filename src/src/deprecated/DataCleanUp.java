package src.deprecated;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * This app clean up the issues with partitioning
 * Will be a temp process until I rewrite EntityGenerator
 * 
 * No longer needed in new engine
 * @param args
 */
@Deprecated
public class DataCleanUp {

	private static String DATA_DIR = "./completed/";
	
	private static String QUOTED_STRING = "`";
	
	private static String SEPERATOR = ",";
	
	// global static variables used in streams
	private static List<String> fileNames = new ArrayList<String>();
	
	private static Set<String> set = new HashSet<String>();
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Path datadir = Paths.get(DATA_DIR);
		/// remove duplicates from table access file
	    try (Stream<Path> paths = Files.walk(datadir)) {
			paths.forEach(p -> {
				fileNames.add(p.getFileName().toString());
			});
	    }
		if(fileNames.contains("TableAccess.csv")) {
	        try (BufferedReader input = Files.newBufferedReader(Paths.get(DATA_DIR + "TableAccess.csv"))) {
        		input.lines().forEach(line ->{
        			set.add(line);
        		});	        }
	        try (BufferedWriter output = Files.newBufferedWriter(Paths.get(DATA_DIR + "TableAccess.csv"), new OpenOption[] { StandardOpenOption.TRUNCATE_EXISTING, WRITE, CREATE })) {
	        		for(String str: set) {
	        			output.write(str + '\n');
	        			output.flush();
	        		}
	        		output.close();
	        }
		}
		/// remove duplicates from i2b2
		set = new HashSet<String>();
		
		if(fileNames.contains("I2B2.csv")) {
	        try (BufferedReader input = Files.newBufferedReader(Paths.get(DATA_DIR + "I2B2.csv"))) {
	        		input.lines().forEach(line ->{
	        			set.add(line);
	        		});
	        		
	        }
	        try (BufferedWriter output = Files.newBufferedWriter(Paths.get(DATA_DIR + "I2B2.csv"), new OpenOption[] { StandardOpenOption.TRUNCATE_EXISTING, WRITE, CREATE })) {
	        		for(String str: set) {
	        			output.write(str + '\n');
	        			output.flush();
	        		}
	        		output.close();
	        }
		}
	}
	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase( "-datadir" )){
				
				DATA_DIR = checkPassedArgs(arg, args);
				
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
