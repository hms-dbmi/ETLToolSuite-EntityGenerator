package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
		
		conceptsWithMaxPatients = findConceptsWithMaxPatients();
		
		Map<String, List<String>> nodesWithAssociatedConceptCds = findNodesToGenerate();
		
		buildCounts(nodesWithAssociatedConceptCds);
		
	}

	private static void buildCounts(Map<String, List<String>> nodesWithAssociatedConceptCds) throws IOException {
		Map<String,List<String>> conceptCdWithPatients = new HashMap<String,List<String>>();
		
		Map<String,Integer> nodeWithPatientCount = new HashMap<String,Integer>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){
			CsvToBean<ObservationFact> csvToBean = Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	

			Iterator<ObservationFact> iter = csvToBean.iterator();
			
			while(iter.hasNext()) {
				ObservationFact fact = iter.next();
				
				if(conceptsWithMaxPatients.contains(fact.getConceptCd())) continue;
								
				if(conceptCdWithPatients.containsKey(fact.getConceptCd())) {
					
					conceptCdWithPatients.get(fact.getConceptCd()).add(fact.getPatientNum());
					
				} else {
					
					conceptCdWithPatients.put(fact.getConceptCd(), new ArrayList<String>(Arrays.asList(fact.getPatientNum())));
				
				};		
			}
		}
		for(Entry<String, List<String>> nodeWcds: nodesWithAssociatedConceptCds.entrySet() ) {
			Set<String> distPatients = new HashSet<String>();
			for(String conceptCd: nodeWcds.getValue()) {
				
				if(conceptCdWithPatients.containsKey(conceptCd)) distPatients.addAll(conceptCdWithPatients.get(conceptCd));
				
				if(distPatients.size() == totalPatients) break;
			}
			if(conceptsWithMaxPatients.contains(nodeWcds)) {
				nodeWithPatientCount.put(nodeWcds.getKey(), totalPatients);
			} else {
				nodeWithPatientCount.put(nodeWcds.getKey(), distPatients.size());
			}
		}
		List<ConceptCounts> ccs = new ArrayList<ConceptCounts>();
		
		Stream<Path> paths = Files.list(Paths.get(PROCESSING_FOLDER));
		
		try(BufferedWriter buffer = openBufferedWriter("ConceptCounts.csv", StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
			StatefulBeanToCsv<ConceptCounts> writer = new StatefulBeanToCsvBuilder<ConceptCounts>(buffer)
					.withQuotechar(DATA_QUOTED_STRING)
					.withSeparator(DATA_SEPARATOR)
					.build();
			
			for(Entry<String, Integer> entry: nodeWithPatientCount.entrySet()) {

				
				int x = StringUtils.countMatches(entry.getKey(), PATH_SEPARATOR);
				
				String parentConceptPath = entry.getKey().substring(0, StringUtils.ordinalIndexOf(entry.getKey(), PATH_SEPARATOR, (x - 1)) + 1);
				
				ConceptCounts cc = new ConceptCounts();
				cc.setConceptPath(entry.getKey());
				cc.setParentConceptPath(parentConceptPath);
				cc.setPatientCount(entry.getValue());
				
			}
			writer.write(ccs);
			
			buffer.flush();
			
			buffer.close();
			
		} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
			
			System.err.println(e);
			
			e.printStackTrace();
			
		} 
	}

	private static Map<String, List<String>> findNodesConceptCds(List<String> nodesToGenerate) throws IOException {
		Map<String, List<String>> nodesWithConceptCds = new HashedMap<String, List<String>>();
		Map<String,String> conceptPathWithCd = new HashMap<String,String>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	

			Iterator<ConceptDimension> iter = csvToBean.iterator();
			
			while(iter.hasNext()) {
				ConceptDimension cd = iter.next();
				conceptPathWithCd.put(cd.getConceptPath(), cd.getConceptCd());
			}
			
		}
		for(String node: nodesToGenerate) {
			List<String> conceptCds = conceptPathWithCd.keySet().stream()
					.filter(key -> key.contains(node))
					.collect(Collectors.toList());
			
			nodesWithConceptCds.put(node, conceptCds);
		}
		
		return nodesWithConceptCds;
	}

	private static Map<String, List<String>> findNodesToGenerate() throws IOException {
		List<String> nodesToGenerate = new ArrayList<String>();
		
		List<String> conceptsWithMaxPatients = findConceptsWithMaxPatients();

		// Map of node and children
		// chlevel only has 1 node that level can be ignored
		// all leaf node counts are created when their variable is created in
		// observation fact generator.
		Map<String,MutableInt> countChlevelNodes = new HashedMap<String, MutableInt>();
		
		List<String> levelsWithSingleCount = null;
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){
			
			CsvToBean<I2B2> csvToBean = 
					Utils.readCsvToBean(I2B2.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
						
			Iterator<I2B2> iter = csvToBean.iterator();
			
			while(iter.hasNext()) {
				I2B2 i2b2 = iter.next();
				
				if(countChlevelNodes.containsKey(i2b2.getcHlevel())) {
					countChlevelNodes.get(i2b2.getcHlevel()).increment();
				} else {
					countChlevelNodes.put(i2b2.getcHlevel(), new MutableInt(1));
				}
				

			}
			
			levelsWithSingleCount = countChlevelNodes.entrySet().stream()
					.filter(entry -> entry.getValue().getValue() != 1)
					.map(entry -> entry.getKey())
					.collect(Collectors.toList());
			
			buffer.close();
		}
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){
			
			CsvToBean<I2B2> csvToBean = 
					Utils.readCsvToBean(I2B2.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
						
			Iterator<I2B2> iter = csvToBean.iterator();
			iter = csvToBean.iterator();
			
			while(iter.hasNext()) {
				I2B2 i2b2 = iter.next();
				
				if(i2b2.getcVisualAttributes().toUpperCase().contains("LA")) continue;
				if(levelsWithSingleCount.contains(i2b2.getcHlevel())) continue;
				if(conceptsWithMaxPatients.contains(i2b2.getcFullName())) continue;
				
				boolean childHasMax = false;
				for(String path: conceptsWithMaxPatients) {
					if(path.contains(i2b2.getcFullName())) {
						childHasMax = true;
						break;
					}
				}
				
				if(childHasMax) {
					conceptsWithMaxPatients.add(i2b2.getcFullName());
					continue;
				}
				
				nodesToGenerate.add(i2b2.getcFullName());
				
			}
			buffer.close();
		}
				
		return findNodesConceptCds(nodesToGenerate);
	}



	private static List<String> findConceptsWithMaxPatients() throws IOException {
		List<String> conceptsWithMaxPatients = new ArrayList<String>();
				
		if(!Files.exists(Paths.get(WRITE_DIR + File.separatorChar + "ConceptCounts.csv"))) {
			System.err.println(WRITE_DIR + File.separatorChar + "ConceptDimension.csv - Does not exist!" );
			return null;
		}
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptCounts.csv"))){
			// build map
			// remove any entries from the map into the concepts with max patients list
			CsvToBean<ConceptCounts> csvToBean = Utils.readCsvToBean(ConceptCounts.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			Iterator<ConceptCounts> iter = csvToBean.iterator();
			
			int totalPatientCount = findTotalPatients();
			
			while(iter.hasNext()) {
				ConceptCounts cc = iter.next();
				if(cc.getPatientCount() != totalPatientCount) continue;
				conceptsWithMaxPatients.add(cc.getConceptPath());
				
			}
			buffer.close();
		}
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			Iterator<ConceptDimension> iter = csvToBean.iterator();
			
			int totalPatientCount = findTotalPatients();
			
			List<String> conceptcds = new ArrayList<String>();
			
			while(iter.hasNext()) {
				ConceptDimension cd = iter.next();
				if(conceptsWithMaxPatients.contains(cd.getConceptPath())) {
					conceptcds.add(cd.getConceptCd());
				}
			}
			
			conceptsWithMaxPatients.addAll(conceptcds);
			buffer.close();
		}
		
		return conceptsWithMaxPatients;
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
	
	public static BufferedWriter openBufferedWriter(String fileName, StandardOpenOption... options) throws IOException {
		return Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + fileName), options);
	}
	
	public static BufferedWriter openBufferedWriter(Path path, StandardOpenOption... options) throws IOException {
		return Files.newBufferedWriter(path, options);
	}
}
