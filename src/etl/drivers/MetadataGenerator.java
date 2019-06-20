package etl.drivers;

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

import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.TableAccess;
import etl.utils.Utils;

public class MetadataGenerator {
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
	
	private static final String C_METADATAXML_NUMERIC = "<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>";
	
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void execute() throws Exception {
		// creates map of root node and data type
		Map<String,String> mappings = createMappings();
		// creates map of concept path and concept cd 
		Map<String,String> concepts = createConcepts();
		
		Set<I2B2> metadata = new HashSet<I2B2>();
		// generate leaf nodes
		generateMetadata(mappings, concepts, metadata);

		// write to disk
		try(BufferedWriter buffer = Files.newBufferedWriter(
				Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"),StandardOpenOption.APPEND)){

			Utils.writeToCsv(buffer, metadata, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 
		// 
	}

	@SuppressWarnings("unused")
	private static void generateMetadata(Map<String, String> mappings, Map<String, String> concepts, Set<I2B2> metadata
			) {

		I2B2 node = new I2B2();
		mappings.entrySet().stream().forEach(mapping ->{
			// if data type is string build string nodes
			if(mapping.getValue().equalsIgnoreCase("NUMERIC")) {
				metadata.add(createNumericNode(mapping,concepts));
			} else if (mapping.getValue().equalsIgnoreCase("TEXT")) {
				metadata.addAll(createTextNodes(mapping,concepts));
			}
			// if numeric build numeric nodes;
		});

		metadata.add(node);
	}

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

	private static Map<String, String> createConcepts() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);	
			
			List<ConceptDimension> concepts = csvToBean.parse();

			Map<String, String> map = concepts.stream().collect(
						Collectors.toMap(ConceptDimension::getConceptPath, ConceptDimension::getConceptCd)
					);
			
			Set<String> conceptNodes = new HashSet<String>();
			return map;
		}
	}

	private static Map<String, String> createMappings() throws IOException {
		List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		Map<String, String> nodes = mappings.stream().collect(
					Collectors.toMap(Mapping::getRootNode, Mapping::getDataType)
				);
		return nodes;
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
