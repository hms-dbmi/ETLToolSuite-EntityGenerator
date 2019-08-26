package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptCounts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2Secure;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.utils.Utils;

public class DataMerge extends Job {

	public static void main(String[] args) {
		try {
			
			mergeCounts();
			
			mergeFacts();
			
			mergeConcepts();
			
			mergeMetadata();
			
			mergeMetadataSecure();
			
		} catch (CsvDataTypeMismatchException e) {
			
			System.err.println(e);
			
		} catch (CsvRequiredFieldEmptyException e) {
			
			System.err.println(e);
			
		} catch (IOException e) {
			
			System.err.println(e);
			
		}
		
	}
	
	private static void mergeConcepts() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		Stream<Path> paths = Files.list(Paths.get(PROCESSING_FOLDER));
		
		List<ConceptDimension> concepts = new ArrayList<ConceptDimension>();
		
		paths.forEach(path -> {
			if(path.getFileName().toString().contains("ConceptDimension.csv")) {
				try(BufferedReader buffer = Files.newBufferedReader(path)){
					CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
					
					concepts.addAll(csvToBean.parse());
					
				} catch (IOException e) {
					System.out.println(e);
				} 
			}
		});
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			StatefulBeanToCsv<ConceptDimension> writer = new StatefulBeanToCsvBuilder<ConceptDimension>(buffer)
					.withQuotechar(DATA_QUOTED_STRING)
					.withSeparator(DATA_SEPARATOR)
					.build();
			
			writer.write(concepts);
		}				
	}

	private static void mergeMetadataSecure() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		Stream<Path> paths = Files.list(Paths.get(PROCESSING_FOLDER));
		
		List<I2B2Secure> metadata = new ArrayList<I2B2Secure>();
		
		paths.forEach(path -> {
			if(path.getFileName().toString().contains("I2B2Secure.csv")) {

				try(BufferedReader buffer = Files.newBufferedReader(path)){
					CsvToBean<I2B2Secure> csvToBean = Utils.readCsvToBean(I2B2Secure.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
				
					metadata.addAll(csvToBean.parse());
				} catch (IOException e) {
					System.out.println(e);
				} 
			}
		});
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "I2B2Secure.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			StatefulBeanToCsv<I2B2Secure> writer = new StatefulBeanToCsvBuilder<I2B2Secure>(buffer)
					.withQuotechar(DATA_QUOTED_STRING)
					.withSeparator(DATA_SEPARATOR)
					.build();
			
			writer.write(metadata);
		}		
	}
	
	private static void mergeMetadata() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		Stream<Path> paths = Files.list(Paths.get(PROCESSING_FOLDER));
		
		List<I2B2Secure> metadata = new ArrayList<I2B2Secure>();
		
		paths.forEach(path -> {
			if(path.getFileName().toString().contains("I2B2.csv")) {
				try(BufferedReader buffer = Files.newBufferedReader(path)){
					CsvToBean<I2B2Secure> csvToBean = Utils.readCsvToBean(I2B2Secure.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
	
					csvToBean.parse().forEach(i2b2 -> {

						metadata.add(i2b2);
					});							
				} catch (IOException e) {
					System.out.println(e);
				} 
			}
		});
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			StatefulBeanToCsv<I2B2Secure> writer = new StatefulBeanToCsvBuilder<I2B2Secure>(buffer)
					.withQuotechar(DATA_QUOTED_STRING)
					.withSeparator(DATA_SEPARATOR)
					.build();
			
			writer.write(metadata);
		}		
	}

	private static void mergeFacts() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		Stream<Path> paths = Files.list(Paths.get(PROCESSING_FOLDER));
		
		paths.forEach(path -> {
			List<ObservationFact> facts = new ArrayList<ObservationFact>();
			
			if(path.getFileName().toString().contains("ObservationFact.csv")) {
				
				try(BufferedReader buffer = Files.newBufferedReader(path)){

					CsvToBean<ObservationFact> csvToBean = Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
	
					facts.addAll(csvToBean.parse());
					
				} catch (IOException e) {
					
					System.out.println(e);
					
				}
				try(BufferedWriter bufferw = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
					
					StatefulBeanToCsv<ObservationFact> writer = new StatefulBeanToCsvBuilder<ObservationFact>(bufferw)
							.withQuotechar(DATA_QUOTED_STRING)
							.withSeparator(DATA_SEPARATOR)
							.build();
					
					try {
						
						writer.write(facts);
						
					} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
						
						System.err.println(e);
						
					}
				} catch (IOException e) {
					
					System.err.println(e);
					 
				}
			}
		});
		

		
	}		
	

	private static void mergeCounts() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		Stream<Path> paths = Files.list(Paths.get(PROCESSING_FOLDER));
		
		List<ConceptCounts> counts = new ArrayList<ConceptCounts>();
		
		paths.forEach(path -> {
			if(path.getFileName().toString().contains("ConceptCounts.csv")) {
				try(BufferedReader buffer = Files.newBufferedReader(path)){
					CsvToBean<ConceptCounts> csvToBean = Utils.readCsvToBean(ConceptCounts.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
	
					counts.addAll(csvToBean.parse());
					
				} catch (IOException e) {
					System.out.println(e);
				} 
			}
		});
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptCounts.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			StatefulBeanToCsv<ConceptCounts> writer = new StatefulBeanToCsvBuilder<ConceptCounts>(buffer)
					.withQuotechar(DATA_QUOTED_STRING)
					.withSeparator(DATA_SEPARATOR)
					.build();
			
			writer.write(counts);
		}
		
	}
	
}
