package etl.jobs.csv;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.job.entity.Mapping;
import etl.jobs.Job;

/** 
 * @author Thomas DeSain
 *
 * This process will clean any data file in the DATA_DIR.
 * This will purge all non ascii characeters as well as a set
 * of unsupported basic latin characters below.
 * 
 * This is a requirement for I2B2/TM to function.
 */
public class DataCurator extends Job{
		
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
	private static List<Character> BAD_CHARS = new ArrayList<Character>(Arrays.asList( '\u002A', 
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
		));
	
	public static void main(String[] args) throws Exception {
		
		setVariables(args, buildProperties(args));
		BAD_CHARS.remove(new Character(DATA_QUOTED_STRING));
		try(DirectoryStream<Path> dirstream = Files.newDirectoryStream(Paths.get(DATA_DIR))){
			Stream<Path> stream = StreamSupport.stream(dirstream.spliterator(), true);

			stream.parallel().forEach(path ->{

				cleanFile(path); 
				try {
					validateRecordLength(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
				
				System.out.println(entry);
				
				try {
					try(BufferedReader reader = Files.newBufferedReader(entry)){
						reader.lines().forEach(line -> {
							
							line = line.trim();
							
							for(char c: line.toCharArray()) {
								
								if( c <= '\u007f' ) {
									
									if(!BAD_CHARS.contains(c)) out.append(c);
									
								} 
								
							}
							
							out.append('\n');
							
						});
					}
					
					Files.write(entry, out.toString().getBytes(StandardCharsets.US_ASCII), WRITE_OPTIONS);
					
				} catch (IOException e) {
					
					System.err.println("error processing " + entry);
					System.err.println(e);		
					e.printStackTrace();
					
				}
			}
		}		
	}
	
	private static void validateRecordLength(Path path) throws IOException {
		List<String[]> badRecords = new ArrayList<String[]>();
		List<String[]> records = new ArrayList<String[]>();
		
		if(!Files.isDirectory(path)) {
			if(Files.isReadable(path)) {
				try(BufferedReader reader = Files.newBufferedReader(path)){
					RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
							.withSeparator(DATA_SEPARATOR)
							.withQuoteChar(DATA_QUOTED_STRING);
						
					RFC4180Parser parser = parserbuilder.build();

					CSVReaderBuilder builder = new CSVReaderBuilder(reader)
							.withCSVParser(parser);
					
					CSVReader csvreader = builder.build();
					
					records = csvreader.readAll();
					
					//if(SKIP_HEADERS) badRecords.add(records.get(0));
					
					if(records.isEmpty()) return;
					
					int expectedLength = records.get(0).length;
					
					for(String[] record: records) {
						if(record.length != expectedLength) {
							badRecords.add(record);
						}
					}
				}
			}
		}
		
		if(!badRecords.isEmpty()) {
			try(BufferedWriter buffer = Files.newBufferedWriter(path, StandardOpenOption.TRUNCATE_EXISTING)){
				RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
						.withSeparator(DATA_SEPARATOR)
						.withQuoteChar(DATA_QUOTED_STRING);
				
				CSVWriterBuilder builder = new CSVWriterBuilder(buffer)
						.withParser(parserbuilder.build());
				
				
				ICSVWriter writer = builder.build();
				
				records.removeAll(badRecords);
				
				writer.writeAll(records);
				writer.flush();
				writer.close();
			}
			String badpathstr = StringUtils.replace(path.toAbsolutePath().toString(), ".csv", ".bad");
			
			
			try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(badpathstr), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
				
				RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
						.withSeparator(DATA_SEPARATOR)
						.withQuoteChar(DATA_QUOTED_STRING);
				
				CSVWriterBuilder builder = new CSVWriterBuilder(buffer)
						.withParser(parserbuilder.build());
				
				ICSVWriter writer = builder.build();
				
				writer.writeAll(badRecords);
				
				writer.flush();
				writer.close();
			}
		}
	}
}
