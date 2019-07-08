package etl.jobs.csv;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** 
 * @author Thomas DeSain
 *
 * This process will clean any data file in the DATA_DIR.
 * This will purge all non ascii characeters as well as a set
 * of unsupported basic latin characters below.
 * 
 * This is a requirement for I2B2/TM to function.
 */
public class DataCurator {

	private static String DATA_DIR = "./data/";
	
	private static String DATA_TYPE = "json";
	
	private static OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, TRUNCATE_EXISTING };	
	/**
	 * List of all Basic Latin characters not supported by I2B2 / TM
	 * 
	 *  * - \u002A
	 *  | - \u007C
	 *  / - \u002F
	 *  < - \u003C
	 *  > - \u003e
	 *  \ - \u005C\u005C twice because it is java's escape char
	 *  " - \u0022
	 *  ' = \u005C\u0027 single Quote needs to be escaped for compiler 
	 *  : - \u003A
	 *  ? - \u003F
	 *  % - \u0025
	 */
	private static List<Character> BAD_CHARS = Arrays.asList( '\u002A', 
			'\u007C', 
			'\u002F', 
			'\u003C',
			'\u005C\u005C',
			'\u0022',
			'\u005C\u0027',
			'\u003A',
			'\u00a0',
			'\u003F',
			'\u003e',
			'\u0025'
		);
	
	public static void main(String[] args) throws Exception {
		
		try(DirectoryStream<Path> dirstream = Files.newDirectoryStream(Paths.get(DATA_DIR))){
			Stream<Path> stream = StreamSupport.stream(dirstream.spliterator(), true);

			stream.parallel().forEach(path ->{
				cleanFile(path); 	
			});

		};
	}

	/**
	 * Iterate over each char in file.  Purge all non Basic Latin chars and 
	 * Illegal application chars.  See badChars variable.
	 * @param entry
	 */
	private static void cleanFile(Path entry) {
		if(!Files.isDirectory(entry)) {
			if(Files.isReadable(entry)) {
				StringBuilder out = new StringBuilder();
				try {

					Files.lines(entry).forEach(line -> {
						line = line.trim();
						for(char c: line.toCharArray()) {
							if( c <= '\u007f' ) {
								if(!BAD_CHARS.contains(c)) out.append(c);
								//else System.err.println(c);
							} else {
								//System.err.println(c);
							}
						}
						out.append('\n');
					});
					
					Files.write(entry, out.toString().getBytes(StandardCharsets.US_ASCII), WRITE_OPTIONS);
					
				} catch (IOException e) {
					System.err.println(e);					
				}
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
