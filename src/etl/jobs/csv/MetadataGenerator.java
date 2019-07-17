package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.TableAccess;
import etl.jobs.jobproperties.JobProperties;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * 
 * This class will generate I2B2 metadata files.
 * 
 * Concept Dimension entities need to be generated before this can be run.
 *
 */
public class MetadataGenerator extends Job {
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	/**
	 * Main method if concepts are already loaded into memory.
	 * @param args
	 * @param concepts
	 * @param jobProperties
	 * @return
	 */
	public static Set<I2B2> main(String[] args, Collection<ConceptDimension> concepts, JobProperties jobProperties) {
		try {
			setVariables(args, jobProperties);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			return execute(concepts);
		} catch (Exception e) {
			System.err.println(e);
		}
		return null;
		
	}
	
	private static void execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
	
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);	
			
			List<ConceptDimension> inputconcepts = csvToBean.parse();		
			// creates map of root node and data type
			Map<String,String> mappings = createMappings();
			// creates map of concept path and concept cd 
			Map<String,String> concepts = createConcepts(inputconcepts);
	
			Set<I2B2> metadata = new HashSet<I2B2>();
			// generate leaf nodes
			generateMetadata(mappings, concepts, metadata);
			
			writeMetadata(metadata);
		}
		
	}
	/**
	 * Wrapper for subprocesses
	 * @return 
	 * 
	 * @throws Exception
	 */
	private static Set<I2B2> execute(Collection<ConceptDimension> inputconcepts) throws Exception {
		// creates map of root node and data type
		Map<String,String> mappings = createMappings();
		// creates map of concept path and concept cd 
		Map<String,String> concepts = createConcepts(inputconcepts);

		Set<I2B2> metadata = new HashSet<I2B2>();
		// generate leaf nodes
		generateMetadata(mappings, concepts, metadata);

		return metadata;

		// 
	}

	public static void writeMetadata(Set<I2B2> metadata) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		// write to disk
		try(BufferedWriter buffer = Files.newBufferedWriter(
				Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)){

			Utils.writeToCsv(buffer, metadata, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 
	}
	
	/**
	 * Generates the I2B2 metadata entities.
	 * 
	 * @param mappings
	 * @param concepts
	 * @param metadata
	 */
	private static void generateMetadata(Map<String, String> mappings, Map<String, String> concepts, Set<I2B2> metadata
			) {

		mappings.entrySet().stream().forEach(mapping ->{
			// if data type is string build string nodes
			if(mapping.getValue().equalsIgnoreCase("NUMERIC")) {
				metadata.add(createNumericNode(mapping,concepts));
			} else if (mapping.getValue().equalsIgnoreCase("TEXT")) {
				metadata.addAll(createTextNodes(mapping,concepts));
			}
			// if numeric build numeric nodes;
		});
	}

	/**
	 * Creates Text nodes
	 * 
	 * @param mapping
	 * @param concepts
	 * @return
	 */
	private static Collection<? extends I2B2> createTextNodes(Entry<String, String> mapping,
			Map<String, String> concepts) {
		Set<I2B2> nodes = new HashSet<I2B2>();
		concepts.entrySet().stream().forEach(concept -> {
			if(concept.getKey().contains(mapping.getKey())) {
				nodes.add(createTextNode(mapping, concept));
			}
		});
		return nodes;
	}

	/**
	 * Create Text nodes
	 * 
	 * @param mapping
	 * @param concept
	 * @return
	 */
	private static I2B2 createTextNode(Entry<String, String> mapping, Entry<String, String> concept) {
		I2B2 node = new I2B2();
		
		node.setcBaseCode(concept.getValue());
		node.setcFullName(concept.getKey());
		node.setcDimCode(concept.getKey());
		node.setcToolTip(concept.getKey());
		String[] nodes = concept.getKey().split("\\\\");
		node.setcName(nodes[nodes.length - 2]);
		node.setcColumnDataType("T");
		node.setcSynonymCd("N");
		node.setcVisualAttributes("LA");
		node.setcHlevel(Integer.toString(StringUtils.countMatches(node.getcFullName(), '\\') - 2));
		node.setcFactTableColumn("CONCEPT_CD");
		node.setcTableName("CONCEPT_DIMENSION");
		node.setcColumnName("CONCEPT_PATH");
		node.setcOperator("LIKE");
		node.setmAppliedPath("@");
		node.setSourceSystemCd(TRIAL_ID);
		
		return node;
	}

	/**
	 * Creates Numeric Nodes
	 * 
	 * @param mapping
	 * @param concepts
	 * @return
	 */
	private static I2B2 createNumericNode(Entry<String, String> mapping, Map<String, String> concepts) {
		I2B2 node = new I2B2();
	
		node.setcBaseCode(concepts.get(mapping.getKey()));
		node.setcFullName(mapping.getKey());
		node.setcDimCode(mapping.getKey());
		node.setcToolTip(mapping.getKey());
		node.setcMetaDataXML(C_METADATAXML_NUMERIC);
		String[] nodes = mapping.getKey().split("\\\\");
		node.setcName(nodes[nodes.length - 2]);
		node.setcColumnDataType("T");
		node.setcSynonymCd("N");
		node.setcVisualAttributes("LA");
		node.setcHlevel(Integer.toString(StringUtils.countMatches(node.getcFullName(), '\\') - 2));
		node.setcFactTableColumn("CONCEPT_CD");
		node.setcTableName("CONCEPT_DIMENSION");
		node.setcColumnName("CONCEPT_PATH");
		node.setcOperator("LIKE");
		node.setmAppliedPath("@");
		node.setSourceSystemCd(TRIAL_ID);
		
		return node;
	}

	/**
	 * Reads the concept dimension entity file generated.
	 * 
	 * @return
	 * @throws IOException
	 */
	private static Map<String, String> createConcepts(Collection<ConceptDimension> concepts) throws IOException {


		Map<String, String> map = concepts.stream().collect(
					Collectors.toMap(ConceptDimension::getConceptPath, ConceptDimension::getConceptCd)
				);
		
		return map;
		//}
	}

	/**
	 * Creates a kv pair using mapping file of path and datatype
	 * 
	 * @return
	 * @throws IOException
	 */
	private static Map<String, String> createMappings() throws IOException {
		List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		Map<String, String> nodes = mappings.stream().collect(
					Collectors.toMap(Mapping::getRootNode, Mapping::getDataType)
				);
		return nodes;
	}

}
