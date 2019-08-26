package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;

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
import etl.job.entity.i2b2tm.PatientDimension;
import etl.utils.Utils;



/**
 * Generates Concept Counts Entity file.
 * 
 * It required that concept dimension and fact entities are generated first and
 * that they are located in the DATA_DIR directory.
 * 
 * @author Thomas DeSain
 *
 */
public class CountGenerator3 extends Job{
	private static int totalPatients = 0;
	
	private static List<String> conceptsWithMaxPatients = null;
	
	static {
		
		try {
			totalPatients = findTotalPatients();
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	/**
	 * Main method that executes subprocesses
	 * Exception handled should happen here.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (IOException e) {
			System.err.println(e);
		} catch (CsvDataTypeMismatchException e) {
			System.err.println(e);
		} catch (CsvRequiredFieldEmptyException e) {
			System.err.println(e);
		}
	}

	/**
	 * Wrapper that calls subprocesses
	 * 
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static void execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		List<ConceptCounts> counts = readCounts();
		
		Map<String,Set<String>> conceptCdAndPatientSet = buildConceptAndPatientSets();
		
		Set<ConceptCounts> newCounts = new HashSet<ConceptCounts>();
		
		for(ConceptCounts count: counts) {
			
			if(new Integer(count.getPatientCount()) == (totalPatients)) {
				
				setAllParentsToMax(newCounts, count);
				
			};
			
		}
		
	}
	
	private static Map<String, Set<String>> buildConceptAndPatientSets() throws IOException {
		Map<String, Set<String>> conceptNPatients = new HashMap<String, Set<String>>();
		
		Map<String, Set<String>> returnMap = new HashMap<String, Set<String>>();
		
		Map<String, String> conceptCdAndPath = conceptCdToConceptPathMapping();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separator + "ObservationFact.csv" ))){
			
			CsvToBean<ObservationFact> csvToBean = Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			conceptNPatients = StreamSupport.stream(csvToBean.spliterator(),false).collect(
					Collectors.groupingBy(ObservationFact::getConceptCd,
							Collectors.mapping(ObservationFact::getPatientNum,Collectors.toSet()))
				);
		}
		
		for(Entry<String,Set<String>> entry: conceptNPatients.entrySet()) {
			
			conceptNPatients.remove(entry);
			
			if(conceptCdAndPath.containsKey(entry.getKey())) {
				returnMap.put(conceptCdAndPath.get(entry.getKey()), entry.getValue());
			}
			
		}
		
		return conceptNPatients;
	}

	private static Map<String, String> conceptCdToConceptPathMapping() throws IOException {
				
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separator + "ConceptDimension.csv" ))){
			
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			return StreamSupport.stream(csvToBean.spliterator(),false).collect(
					Collectors.toMap(ConceptDimension::getConceptCd, ConceptDimension::getConceptPath)
				);
		}
	}

	private static void setAllParentsToMax(Set<ConceptCounts> newCounts, ConceptCounts count) {
		
		int countNodes = StringUtils.countMatches(count.getConceptPath(), PATH_SEPARATOR) - 2;
		
		while(countNodes > 0) {
			
			String path = StringUtils.substring(
					count.getConceptPath(), 
					0, 
					StringUtils.ordinalIndexOf(
							count.getConceptPath(), 
							PATH_SEPARATOR,
							StringUtils.countMatches(count.getConceptPath(), PATH_SEPARATOR) - 1)
					);
			
			String parentPath = StringUtils.substring(
					count.getConceptPath(), 
					0, 
					StringUtils.ordinalIndexOf(
							count.getConceptPath(), 
							PATH_SEPARATOR,
							StringUtils.countMatches(count.getConceptPath(), PATH_SEPARATOR) - 2)
					);
			
			ConceptCounts cc = new ConceptCounts();
		}
		
	}

	private static List<ConceptCounts> readCounts() throws IOException {
		
		if(!Files.exists(Paths.get(WRITE_DIR + File.separator + "ConceptCounts.csv"))) throw new FileNotFoundException();
				
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separator + "ConceptCounts.csv" ))){
			
			CsvToBean<ConceptCounts> csvToBean = Utils.readCsvToBean(ConceptCounts.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			return csvToBean.parse();
		}
	}

	private static int findTotalPatients() throws IOException {
		
		if(!Files.exists(Paths.get(WRITE_DIR + File.separatorChar + "PatientDimension.csv"))) {
			
			System.err.println(WRITE_DIR + File.separatorChar + "PatientDimension.csv - Does not exist!" );
			
			return -1;
			
		}
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "PatientDimension.csv"))){
			CsvToBean<PatientDimension> csvToBean = Utils.readCsvToBean(PatientDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			int x = csvToBean.parse().size();
			buffer.close();
			return x;
			 
		}
	}
}