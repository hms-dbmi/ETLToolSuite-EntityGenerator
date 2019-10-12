package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * This class purpose is to process a data file and generate
 * the Concept Dimension entity file that can be loaded into a data store
 * 
 */
public class ConceptGenerator extends Job{
	/**
	 * Standalone Main so this class can generate concepts alone.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
			System.err.println(e);
		}
		
		try {
			writeConcepts(execute());
		} catch (CsvDataTypeMismatchException e) {
			System.err.println(e);
		} catch (CsvRequiredFieldEmptyException e) {
			System.err.println(e);
		} catch (IOException e) {
			System.err.println(e);
		}
	}
	/**
	 * Main method that drives Concept Generation Process
	 * 
	 * Exceptions should be caught and handled here.
	 * 
	 * @param args
	 * @return 
	 */
	public static Collection<ConceptDimension> main(String[] args, JobProperties jobProperties) {
		try {
			setVariables(args, jobProperties);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.out.println(e.toString());
			e.printStackTrace();
		}
		
		try {
			return execute();
		} catch (CsvDataTypeMismatchException e) {
			System.err.println(e);
			e.printStackTrace();
		} catch (CsvRequiredFieldEmptyException e) {
			System.err.println(e);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println(e);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Wrapper that calls 
	 * 
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static Collection<ConceptDimension> execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		List<Mapping> mappings = new ArrayList<Mapping>();
		
		mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		HashSet<ConceptDimension> setCds = new HashSet<ConceptDimension>();

		doConceptReader(setCds, mappings);
		
		return setCds;
			
	}

	/**
	 * Method generates concepts via mapping file.
	 * 
	 * @param setCds
	 * @param mappings
	 * @throws IOException
	 */
	private static void doConceptReader(Collection<ConceptDimension> cds, List<Mapping> mappings) throws IOException {
			
		mappings.stream().forEach(mapping -> {
			if(mapping.getKey().split(":").length < 2) return;
			
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
				
				records.forEach(record ->{
					if(record.length - 1 < column) return;
					if(record[column].isEmpty()) {
						return;
					}
					
					ConceptDimension cd = new ConceptDimension();
					
					String conceptCd = mapping.getDataType().equalsIgnoreCase("numeric") ?
							mapping.getRootNode() :mapping.getRootNode() + record[column] + PATH_SEPARATOR; 
					
					String conceptPath = mapping.getDataType().equalsIgnoreCase("numeric") ? 
							mapping.getRootNode() :mapping.getRootNode() + record[column] + PATH_SEPARATOR; 
							
					cd.setConceptCd(conceptCd);
					cd.setConceptPath(conceptPath);

					if(mapping.getDataType().equalsIgnoreCase("numeric")) {
						String[] nodes = StringUtils.split(conceptPath, PATH_SEPARATOR.charAt(0));
						cd.setNameChar(nodes[nodes.length -1]);
					} else {
						cd.setNameChar(record[column]);
					}
					cd.setSourceSystemCd(TRIAL_ID);

					cds.add(cd);
					
				});
			} catch (IOException e) {
				System.err.println(e);
				e.printStackTrace();
			}
			
		});	
	
	}

	/**
	 * Writes the generated concepts to a flat file.
	 * 
	 * @param setCds
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	public static void writeConcepts(Collection<ConceptDimension> setCds,StandardOpenOption... options) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(PROCESSING_FOLDER + File.separatorChar + CONFIG_FILENAME + '_' + "ConceptDimension.csv"),options)){
			
			Utils.writeToCsv(buffer, setCds.stream().collect(Collectors.toList()), DATA_QUOTED_STRING, DATA_SEPARATOR);
			buffer.flush();
			buffer.close();
		} 		
	}
	
}
