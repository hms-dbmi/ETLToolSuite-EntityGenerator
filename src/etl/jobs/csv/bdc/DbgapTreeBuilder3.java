package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import etl.jobs.mappings.Mapping;


/**
 * Meant to ingest one Study at a time and generate a dynamic mapping file.
 * 
 * Required:
 * Data and Data_Dict files must be located in DATA_DIR ( ./data/ by default ) 
 * Managed Input must contain STUDY_ID and ACCESSION
 * Studies job.config must be in RESOURCES_DIR ( ./resources/ by default )
 * 
 * @author Thomas DeSain
 *
 */

public class DbgapTreeBuilder3 extends BDCJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -445541832610664833L;
    private static final Log log = LogFactory.getLog(DbgapTreeBuilder3.class);


    private static boolean PROCESS_MISSING_DICTIONARY = false;

	private static Map<String, List<String>> missingHeaders;


	
	private static TreeSet<String> NUMERIC_DATA_TYPES = new TreeSet<>();
	
	private static TreeSet<String> TEXT_DATA_TYPES = new TreeSet<>();
	
	private static TreeSet<String> DICTIONARY_KEYS = new TreeSet<>();

	private static TreeSet<String> VARIABLES_MISSING_DATA_TYPE = new TreeSet<>();

	private static TreeSet<String> FILES_MISSING_DICTIONARIES = new TreeSet<>();

	static {
		NUMERIC_DATA_TYPES.add("numeric");
		NUMERIC_DATA_TYPES.add("integer");
		NUMERIC_DATA_TYPES.add("numeral");
		NUMERIC_DATA_TYPES.add("decimal, encoded");
		NUMERIC_DATA_TYPES.add("decimal, encoded value");
		NUMERIC_DATA_TYPES.add("number");
		NUMERIC_DATA_TYPES.add("num");
		NUMERIC_DATA_TYPES.add("real, encoded value");
		NUMERIC_DATA_TYPES.add("1");
		NUMERIC_DATA_TYPES.add("9");
		
		
		TEXT_DATA_TYPES.add("encoded");  
		TEXT_DATA_TYPES.add("string"); 
		TEXT_DATA_TYPES.add("string (numeral)"); 
		TEXT_DATA_TYPES.add("char");
		TEXT_DATA_TYPES.add("coded"); 
		TEXT_DATA_TYPES.add("encoded,string"); 
		TEXT_DATA_TYPES.add("encoded value");
		TEXT_DATA_TYPES.add("string, encoded value"); 
		TEXT_DATA_TYPES.add("year"); 
		TEXT_DATA_TYPES.add("datetime"); 
		TEXT_DATA_TYPES.add("character"); 
		TEXT_DATA_TYPES.add("enum_integer"); 
		TEXT_DATA_TYPES.add("empty field"); 
		TEXT_DATA_TYPES.add("encoded values"); 
		TEXT_DATA_TYPES.add("integer, encoded value");
		TEXT_DATA_TYPES.add("integer, encoded"); 
	}
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		try {
			execute();
		} catch (IOException | ParserConfigurationException | SAXException e) {
			
			System.err.println(e);
			e.printStackTrace();
		}	
	}
	
	private static void execute() throws IOException, ParserConfigurationException, SAXException {
		// preserve root node from mapping file
		//if(PRESERVE_ROOT_NODE) setRootNodeFromPreviousMapping();
		
		//else setRootNodeFromManagedInputs();

		// Build the mapping file.
		buildMappingFile();
		
	}
	
	private static void buildMappingFile() throws IOException, ParserConfigurationException, SAXException {
		
		File dir = new File(DATA_DIR);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_mapping2.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			if(dir.isDirectory()) {
				
				for(File file: dir.listFiles()) {
					
					if(!file.getName().toLowerCase().startsWith(TRIAL_ID.toLowerCase())) continue;
					if(!file.getName().endsWith("txt")) continue;
					//if(file.getName().toUpperCase().contains("MULTI")) continue;
	
					String pht = getPht(file);
					
					Document dictionary = getDictionary(pht);
					
					
					if(dictionary == null) {

						System.err.println("Missing dictionary file for " + file.getName());
						FILES_MISSING_DICTIONARIES.add(file.getName());
						if(PROCESS_MISSING_DICTIONARY == false) continue;
					
					}
					//
					collectDictionaryKeys(dictionary);

					String[] headers = getDataHeaders(file);
					if(headers == null) continue;
					if(headers.length == -1) continue;
					missingHeaders = findMissingHeadersInDictionary(dictionary,headers);
					
					generateMappings(buffer, file, headers, dictionary);
					
				}
				
			}
			buffer.flush();
			buffer.close();
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(LOG_DIR + TRIAL_ID + "_DictionaryKeys.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String dk: DICTIONARY_KEYS) {
				buffer.write(dk + '\n');
			}
			buffer.flush();
			buffer.close();
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(LOG_DIR + TRIAL_ID + "_MissingDataTypes.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String mdt: VARIABLES_MISSING_DATA_TYPE) {
				buffer.write(mdt + '\n');
			}
			buffer.flush();
			buffer.close();
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(LOG_DIR + TRIAL_ID + "_FilesMissingDictionaries.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String fmd: FILES_MISSING_DICTIONARIES) {
				buffer.write(fmd + '\n');
			}
			buffer.flush();
			buffer.close();
		}
		if(FILES_MISSING_DICTIONARIES.size()>0 || VARIABLES_MISSING_DATA_TYPE.size()>0)
			System.exit(255);
	}

	private static void generateMappings(BufferedWriter buffer, File file, String[] headers, Document dictionary) throws IOException {
		// get the description from root data table
		

		
		String phs = file.getName().split("\\.")[0];
		ROOT_NODE = phs;
		
		if(ROOT_NODE == null) {
			throw new IOException("root node not found in managed input for " + TRIAL_ID);
		}
		for(String header: headers) {
			//if(file.getName().toUpperCase().contains("MULTI")) continue;
			Mapping mapping = new Mapping();
			
			boolean isHeaderMissing = false;
			
			String pht = getPht(file);
			
			if(missingHeaders.containsKey(pht)) {
				if( missingHeaders.get(pht).contains(header)) isHeaderMissing = true;
			}
			
			if(!isHeaderMissing) {
				
				//String varDesc = findVariableDescription(header,dictionary);
				
				String phv = findVariablePHV(header, dictionary);
				
				if(phv != null) {
				
					StringBuilder conceptPathSB = new StringBuilder();
						
					conceptPathSB.append(PATH_SEPARATOR);
	
					conceptPathSB.append(ROOT_NODE);
	
					conceptPathSB.append(PATH_SEPARATOR);
										
					conceptPathSB.append(pht);
					
					conceptPathSB.append(PATH_SEPARATOR);
					
					conceptPathSB.append(phv.replaceAll("\\..*", ""));
				
					conceptPathSB.append(PATH_SEPARATOR);
					
					conceptPathSB.append(header);
					
					conceptPathSB.append(PATH_SEPARATOR);
					
					mapping.setKey(file.getName() + ":" + Arrays.asList(headers).indexOf(header));
					
					mapping.setRootNode(conceptPathSB.toString());
					
					mapping.setDataType(findDataType(dictionary));

					if(mapping.getDataType() == null) {
                        System.err.println("Missing data type for " + header);
						System.err.println("Data Type invalid or missing in data dictionary: " + 
								"TRIAL_ID=" + TRIAL_ID + 
								",pht=" + pht +
								",phv=" + header);
						VARIABLES_MISSING_DATA_TYPE.add(phs + "," + pht + "," + header);
					} else if (mapping.getDataType().isEmpty()) {
                        System.err.println("Data Type empty in data dictionary: " +
                                "TRIAL_ID=" + TRIAL_ID +
                                ",pht=" + pht +
                                ",phv=" + header
                        + ", setting to TEXT");
                        mapping.setDataType("TEXT");
                    }
				}
			} else {
				System.err.println("Header missing in data dictionary: " + 
						"TRIAL_ID=" + TRIAL_ID + 
						"pht=" + pht +
						"phv=" + header);
			}
			
			if(mapping.getRootNode().split(PATH_SEPARATOR.toString()).length > 2)  {
				System.out.println(mapping.toCSV());
				buffer.write(mapping.toCSV() + "\n");
				buffer.flush();
			
			}
		}
		
	}

	
	private static String findVariablePHV(String header, Document dictionary) {
		
		NodeList variables = dictionary.getElementsByTagName("variable");
		
		NodeList dataTable = dictionary.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String id = nodeMap.getNamedItem("id").getNodeValue(); //.replaceAll("\\..*", "");
		
		if(missingHeaders.get(id).contains(header)) return null;
		
		String name = "";
		
		String phv = "";
		
		for (int idx = 0; idx < variables.getLength(); idx++) {
	    	
	    		Node node = variables.item(idx);
	    		
	    		NodeList variableChildren = node.getChildNodes();
	    		
	    		
	    		for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
	    			
	        		Node node2 = variableChildren.item(idx2);
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("name")) {
		        		
	        			name = node2.getTextContent();	     
	        			
	        			if(!name.equalsIgnoreCase(header)) {
	        				name = "";
	        				continue;
	        			} else {
	        				phv = node.getAttributes().getNamedItem("id").getNodeValue();
	        				return phv;
	        			}
	        			
	        		}
	        		
	        		
	        		if(!name.isEmpty() && !phv.isEmpty() && name.equalsIgnoreCase(header)) break;	        		
	    		}
	    		if(!name.isEmpty() && !phv.isEmpty() && name.equalsIgnoreCase(header)) return phv;
		}
		if(phv.isEmpty()) return header;
		
		return phv;
		
	}
	
	
	private static Map<String, List<String>> findMissingHeadersInDictionary(Document dictionary, String[] headers) {
		Map<String, List<String>> missingHeaders = new HashMap<>();
		
		List<String> variablesInDictionary = new ArrayList<>();

		NodeList variables = dictionary.getElementsByTagName("variable");
		
		NodeList dataTable = dictionary.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String id = nodeMap.getNamedItem("id").getNodeValue();

	    for (int idx = 0; idx < variables.getLength(); idx++) {
	    	
	    		Node node = variables.item(idx);
	    		
	    		NodeList variableChildren = node.getChildNodes();
	    		
	    		for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
	
	        		Node node2 = variableChildren.item(idx2);
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("name")) {
	        		
	        			String name = node2.getTextContent();

	        			variablesInDictionary.add(name);
	        			
	        			
	        		}
	        			        		
	    		}
	    }
		List<String> missing = new ArrayList<String>();

		for(String header: headers) {
			if(variablesInDictionary.contains(header)) {
				continue;
			} else {
				missing.add(header);
			}
			
		}
		missingHeaders.put(id, missing);
		return missingHeaders;
	}
	

	
	private static String getPht(File file) {
		if(file.getName().startsWith("phs")) {
		
			return file.getName().split("\\.")[2];
			
		} 
		return null;
	}
	
	private static Document getDictionary(String pht) throws ParserConfigurationException, SAXException, IOException {
		
		File dir = new File(DATA_DIR);
		
		File[] files = dir.listFiles(new FileFilter() {

		    @Override
		    public boolean accept(File file) {
		    		if(file.getName().contains(pht)) {
		    			return file.getName().contains(".xml");
		    		}
		        return false;
		        
		    }
		
		});
		
		for(File f: files) {
			if(f.getName().contains(pht)) {
				if(f.getName().contains("data_dict")) {
					DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
					DocumentBuilder builder = factory.newDocumentBuilder();
					
					return builder.parse(f);
				}
			}
		}
		return null;
	}
	


	private static String findDataType(Document dataDic) {
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();
		
		String type = "";
		
		//if(USE_DESC == false) return desc;
		for(int x = 0 ; x < dataNodes.getLength() ; x++ ) {
			
			Node n = dataNodes.item(x);
			if(n.getNodeName().equals("variable")) {
				
				dataNodes = n.getChildNodes();
				
				for(int y = 0 ; y < dataNodes.getLength() ; y++ ) {
					n = dataNodes.item(y);
					if(n.getNodeName().equals("type")) {
						type = n.getTextContent();

                        // If the type is empty, but the variable is present, we assume it's a text type.
						if(type == null) {
							return "TEXT";
						}

						type = type.replace("\"", "'");
						type = type.replace("\\", "");
						if(NUMERIC_DATA_TYPES.contains(type.toLowerCase())) return type = "NUMERIC";
						else if(TEXT_DATA_TYPES.contains(type.toLowerCase())) return type = "TEXT"; 
						else return type = null; // return null if the type is not recognized and wasn't null
					}
				}

                System.out.println("Skipping node " + n.getNodeName() + " in data dictionary for " + TRIAL_ID + " as it is not a type node.");
			} else {
                System.out.println("Skipping node " + n.getNodeName() + " in data dictionary for " + TRIAL_ID + " as it is not a variable node.");
            }
		}

		return type;
	}


	private static void collectDictionaryKeys(Document dataDic) {
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();
		
		//if(USE_DESC == false) return desc;
		for(int x = 0 ; x < dataNodes.getLength() ; x++ ) {
			
			Node n = dataNodes.item(x);
			
			if(n.getNodeName() != null || !n.getNodeName().isEmpty()) {
				DICTIONARY_KEYS.add(n.getNodeName());
			}
			
			Node n2 = dataNodes.item(x);
			if(n2.getNodeName().equals("variable")) {
				
				NodeList dataNodes2 = n2.getChildNodes();
				
				for(int y = 0 ; y < dataNodes2.getLength() ; y++ ) {
					n2 = dataNodes2.item(y);
					if(n2.getNodeName() != null || !n2.getNodeName().isEmpty()) {
						DICTIONARY_KEYS.add(n2.getNodeName());
					}
					
				}
			}
			else if(n2.getNodeName().equals("description")) {
				
				NodeList dataNodes2 = n2.getChildNodes();
				
				for(int y = 0 ; y < dataNodes2.getLength() ; y++ ) {
					n2 = dataNodes2.item(y);
					if(n2.getNodeName() != null || !n2.getNodeName().isEmpty()) {
						DICTIONARY_KEYS.add(n2.getNodeName());
					}
					
				}
			
			}
		}

	}
}
