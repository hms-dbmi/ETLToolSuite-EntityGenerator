package etl.drivers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
import etl.utils.Utils;

public class CountGenerator {
	private static boolean SKIP_HEADERS = false;

	private static String DATA_DIR = "./completed/";
	
	private static final char DATA_SEPARATOR = ',';

	private static final char DATA_QUOTED_STRING = '"';
		
	private static String TRIAL_ID = "DEFAULT";
	
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvDataTypeMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvRequiredFieldEmptyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		// hashmap containing conceptcd and patientset
		Map<String, Set<String>> factmap = null;
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){

			CsvToBean<ObservationFact> csvToBean = Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);	
		
			List<ObservationFact> facts = csvToBean.parse();
			
			factmap =
					facts.stream().collect(Collectors.groupingBy(
							ObservationFact::getConceptCd,
								Collectors.mapping(ObservationFact::getPatientNum, Collectors.toSet())
								)
							);
			
			//CountNode.gatherPatientNums(facts);
		}
		// hashmap with conceptpath and merging all patients sets from the fact map
		
		List<ConceptCounts> ccCounts = new ArrayList<ConceptCounts>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ConceptDimension.csv"))){
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);	
			
			List<ConceptDimension> concepts = csvToBean.parse();

			Map<String, String> conceptCdSetMap = concepts.stream().collect(
						Collectors.toMap(ConceptDimension::getConceptPath, ConceptDimension::getConceptCd)
					);
			
			Set<String> conceptNodes = new HashSet<String>();

			conceptCdSetMap.keySet().forEach(concept ->{
				String currentPath = concept;
				conceptNodes.add(currentPath);

				int x = StringUtils.countMatches(currentPath, '\\');
				
				while(x > 2) {
					
					currentPath = concept.substring(0, StringUtils.ordinalIndexOf(concept, "\\", (x - 1)) + 1);
					
					conceptNodes.add(currentPath);
					
					x = StringUtils.countMatches(currentPath, '\\');
				}
			});
			// ConceptPath and Patient Set
			
			ccCounts = compileNodes(conceptNodes,conceptCdSetMap,factmap);
			
		}		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(DATA_DIR + File.separatorChar + "ConceptCounts.csv"))){

			Utils.writeToCsv(buffer, ccCounts, DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
	}

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
			int x = StringUtils.countMatches(entry.getKey(), '\\');
			cc.setParentConceptPath(entry.getKey().substring(0, StringUtils.ordinalIndexOf(entry.getKey(), "\\", (x - 1)) + 1));
			cc.setPatientCount(entry.getValue().size());
			ccList.add(cc);
		});
		
		return ccList;
	}

	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-skipheaders")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					SKIP_HEADERS = true;
				} 
			}

			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
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
