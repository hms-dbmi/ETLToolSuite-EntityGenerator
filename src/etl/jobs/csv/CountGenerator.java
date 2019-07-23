package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptCounts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.jobproperties.JobProperties;
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
public class CountGenerator extends Job{

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
		// hashmap containing conceptcd and patientset
		Map<String, Set<String>> factmap = readFactFile();
		
		List<ConceptCounts> ccCounts = new ArrayList<ConceptCounts>();
		
		generateCounts(ccCounts, factmap);

	}

	/**
	 * Generates the concept counts using the concept dimension file.
	 * Will recursively break down all paths into smaller nodes and use the factmap to find all
	 * associated conceptcds and patient sets.
	 * Will merge all patient sets into a hashset to create a distinct collection of patient nums whom's
	 * size will result in the concept counts
	 * 
	 * @param ccCounts
	 * @param factmap
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static void generateCounts(List<ConceptCounts> ccCounts, Map<String, Set<String>> factmap) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			List<ConceptDimension> concepts = csvToBean.parse();

			Map<String, String> conceptCdSetMap = concepts.stream().collect(
						Collectors.toMap(ConceptDimension::getConceptPath, ConceptDimension::getConceptCd)
					);
			
			Set<String> conceptNodes = new HashSet<String>();

			conceptCdSetMap.keySet().forEach(concept ->{
				String currentPath = concept;
				conceptNodes.add(currentPath);

				int x = StringUtils.countMatches(currentPath, PATH_SEPARATOR);
				
				while(x > 2) {
					
					currentPath = concept.substring(0, StringUtils.ordinalIndexOf(concept, PATH_SEPARATOR, (x - 1)) + 1);
					
					conceptNodes.add(currentPath);
					
					x = StringUtils.countMatches(currentPath, PATH_SEPARATOR);
				}
			});
			// ConceptPath and Patient Set
			
			ccCounts = compileNodes(conceptNodes,conceptCdSetMap,factmap);
			
		}		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptCounts.csv"))){

			Utils.writeToCsv(buffer, ccCounts, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 		
	}

	/**
	 * Method loads a map of concept codes with a set of associated patient nums.
	 * This will be used as a lookup when generating counts.
	 * 
	 * @param factmap
	 * @return 
	 * @throws IOException
	 */
	private static Map<String, Set<String>> readFactFile() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){

			CsvToBean<ObservationFact> csvToBean = Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
		
			List<ObservationFact> facts = csvToBean.parse();
			
			Map<String, Set<String>> factmap = facts.stream().collect(Collectors.groupingBy(
							ObservationFact::getConceptCd,
								Collectors.mapping(ObservationFact::getPatientNum, Collectors.toSet())
								)
							);
			return factmap;
		}
	}

	/**
	 * Methods takes concept nodes and generates an array list of concept counts.
	 * 
	 * @param conceptNodes
	 * @param conceptCdSetMap
	 * @param factmap
	 * @return
	 */
	private static List<ConceptCounts> compileNodes(Set<String> conceptNodes, Map<String, String> conceptCdSetMap,
			Map<String, Set<String>> factmap) {
		
		Map<String, Set<String>> conceptPatSetMap = new HashMap<String, Set<String>>();
		List<ConceptCounts> ccList = new ArrayList<ConceptCounts>();
		conceptNodes.stream().forEach(node ->{
			Set<String> conceptCds = new HashSet<String>();
			conceptCdSetMap.keySet().stream().forEach(key ->{
				if(key.contains(node)) conceptCds.add(conceptCdSetMap.get(key));
			});
			Set<String> patientNums = new HashSet<String>();
			conceptCds.stream().forEach(conceptCd -> {
				if(factmap.containsKey(conceptCd)) {
					patientNums.addAll(factmap.get(conceptCd));
				}
			});
			conceptPatSetMap.put(node, patientNums);
		});
		conceptPatSetMap.entrySet().stream().forEach(entry ->{
			ConceptCounts cc = new ConceptCounts();
			cc.setConceptPath(entry.getKey());
			int x = StringUtils.countMatches(entry.getKey(), PATH_SEPARATOR);
			cc.setParentConceptPath(entry.getKey().substring(0, StringUtils.ordinalIndexOf(entry.getKey(), PATH_SEPARATOR, (x - 1)) + 1));
			cc.setPatientCount(entry.getValue().size());
			ccList.add(cc);
		});
		
		return ccList;
	}
}
