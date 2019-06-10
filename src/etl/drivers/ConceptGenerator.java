package etl.drivers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.PatientDimension;
import etl.job.entity.i2b2tm.PatientTrial;
import etl.utils.Utils;

public class ConceptGenerator {
	private static final List<LinkOption> options = null;
	
	private static boolean SKIP_HEADERS = false;

	private static String WRITE_DIR = "./completed/";
	
	private static String MAPPING_FILE = "./mappings/mapping.csv";

	private static boolean MAPPING_SKIP_HEADER = false;

	private static char MAPPING_DELIMITER = ',';

	private static char MAPPING_QUOTED_STRING = '"';

	private static final char DATA_SEPARATOR = ',';

	private static final char DATA_QUOTED_STRING = '"';
	
	private static String DATA_DIR = "./data/";

	private static String TRIAL_ID = "DEFAULT";
	
	public static void main(String[] args) throws Exception {
		setVariables(args);
		
		execute();
	}
	
	private static void execute() {
		List<Mapping> mappings = new ArrayList<Mapping>();
		// generate mapping
		try {
			mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error generating mapping file!");
			System.err.println(e.getMessage());
		}
		
		Set<ConceptDimension> setCds = new HashSet<ConceptDimension>();
		
		try {
			doConceptReader(setCds, mappings);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error generating Concepts!");
			System.err.println(e.getMessage());		
		}
		try {
			writeConcepts(setCds);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error writing Concepts!");
			System.err.println(e.getMessage());		
		} catch (CsvDataTypeMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvRequiredFieldEmptyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void writeConcepts(Set<ConceptDimension> setCds) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
			Utils.writeToCsv(buffer, setCds.stream().collect(Collectors.toList()), DATA_QUOTED_STRING, DATA_SEPARATOR);
			
		} 		
	}
	public static int mappingcount = 0;
	
	private static void doConceptReader(Set<ConceptDimension> setCds, List<Mapping> mappings) throws IOException {
		Integer mappingSize = mappings.size();
		
		mappings.stream().parallel().forEach(mapping -> {

			String fileName = mapping.getKey().split(":")[0];
			Integer column = new Integer(mapping.getKey().split(":")[1]);
			
			try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + fileName))){
				RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
						.withSeparator(DATA_SEPARATOR)
						.withQuoteChar(DATA_QUOTED_STRING);
					
				RFC4180Parser parser = parserbuilder.build();

				CSVReaderBuilder builder = new CSVReaderBuilder(reader)
						.withCSVParser(parser);
				
				CSVReader csvreader = builder.build();
				
				if(SKIP_HEADERS) csvreader.readNext();

				List<String[]> records = csvreader.readAll();
				
				records.parallelStream().forEach(record ->{
					ConceptDimension cd = new ConceptDimension();
					
					String conceptCd = mapping.getDataType().equalsIgnoreCase("numeric") ?
							mapping.getRootNode() :mapping.getRootNode() + record[column] + "\\"; 
					
					String conceptPath = mapping.getDataType().equalsIgnoreCase("numeric") ? 
							mapping.getRootNode() :mapping.getRootNode() + record[column] + "\\"; 
							
					cd.setConceptCd(conceptCd);
					cd.setConceptPath(conceptPath);
					cd.setNameChar(record[column]);
					cd.setSourceSystemCd(TRIAL_ID);
					
					setCds.add(cd);
				});
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println(e.getMessage());
			}
			
		});		
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
