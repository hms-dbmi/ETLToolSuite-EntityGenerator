package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
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

public class DbgapTreeBuilder2 extends BDCJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -445541832610664833L;

	private static final boolean PRESERVE_ROOT_NODE = false;
	
	private static boolean PROCESS_MISSING_DICTIONARY = false;

	private static Map<String, List<String>> missingHeaders;
	// root nodes per accession
	private static Map<String, String> rootNodes = new HashMap<>();

	private static boolean LABEL_IS_ENCODED;
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			setLocalVariables(args);
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
		if(PRESERVE_ROOT_NODE) setRootNodeFromPreviousMapping();
		
		else setRootNodeFromManagedInputs();

		// Build the mapping file.
		buildMappingFile();
		
	}
	
	private static void setRootNodeFromManagedInputs() throws IOException {
		
		List<BDCManagedInput> managedInputs = getManagedInputs();
		
		for(BDCManagedInput managedInput: managedInputs) {
			if(TRIAL_ID.trim().equalsIgnoreCase(managedInput.getStudyAbvName().trim())) {
				
				rootNodes.put(managedInput.getStudyIdentifier(), managedInput.getStudyFullName() + " ( " + managedInput.getStudyIdentifier() + " )");

			}
		}
	}

	private static void setRootNodeFromPreviousMapping() throws IOException {
		
		if(Files.exists(Paths.get(MAPPING_FILE))) {
		
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(MAPPING_FILE))) {
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] line;
				
				while((line = reader.readNext()) != null) {
					
					if(line[0].contains(":")) {
						String[] str = line[1].split(PATH_SEPARATOR.toString());
						if(str.length > 1) {
							ROOT_NODE = str[1];
						} else {
							if(ROOT_NODE.equalsIgnoreCase("DEFAULT")) throw new IOException("no root node specified");
						}
						
						break;
					}
					
				}
			}
			
		}
		
	}

	private static void buildMappingFile() throws IOException, ParserConfigurationException, SAXException {
		
		File dir = new File(DATA_DIR);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
		
			if(dir.isDirectory()) {
				
				for(File file: dir.listFiles()) {
					
					if(!file.getName().startsWith("phs")) continue;
					if(!file.getName().endsWith("txt")) continue;
					//if(file.getName().toUpperCase().contains("MULTI")) continue;

					String pht = getPht(file);
					
					Document dictionary = getDictionary(pht);
					
					if(dictionary == null) {
						
						System.err.println("Missing dictionary file for " + file.getName());
						if(PROCESS_MISSING_DICTIONARY == false) continue;
					
					}
					//
					
					String[] headers = getDataHeaders(file);
					if(headers == null) continue;
					missingHeaders = findMissingHeadersInDictionary(dictionary,headers);
					
					generateMappings(buffer, file, headers, dictionary);
					
				}
				
			}
			
		}
	}
	
	private static void generateMappings(BufferedWriter buffer, File file, String[] headers, Document dictionary) throws IOException {
		// get the description from root data table
		
		String desc = findDescription(dictionary);
		int x = 0;
		
		List<Mapping> mappings = new ArrayList<>();
		
		if(!PRESERVE_ROOT_NODE) {
			String phs = file.getName().split("\\.")[0];
			ROOT_NODE = rootNodes.get(phs);
			if(!phs.contains("phs")) throw new IOException("BAD File " + phs); 
		}
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
				
				String varDesc = findVariableDescription(header,dictionary);
				
				if(varDesc != null) {
				
					StringBuilder conceptPathSB = new StringBuilder();
						
					conceptPathSB.append(PATH_SEPARATOR);

					conceptPathSB.append(ROOT_NODE);

					conceptPathSB.append(PATH_SEPARATOR);
										
					if(!desc.replaceAll("\"", "").isEmpty()) {
						conceptPathSB.append(desc.replaceAll("\"", ""));
					
						conceptPathSB.append(PATH_SEPARATOR);
					}
					
					if(!varDesc.replaceAll("\"", "").isEmpty()) {
						conceptPathSB.append(varDesc.replaceAll("\"", ""));
					
						conceptPathSB.append(PATH_SEPARATOR);
					}
					
					mapping.setKey(file.getName() + ":" + x);
					
					mapping.setRootNode(conceptPathSB.toString());
					
					mapping.setDataType("TEXT");
					
				} else {
					
					// dont create mapping if missing var description
					continue; 
					/*
					StringBuilder conceptPathSB = new StringBuilder();
					
					conceptPathSB.append(PATH_SEPARATOR);
	
					conceptPathSB.append(ROOT_NODE);
					
					conceptPathSB.append(PATH_SEPARATOR);
					
					if(!desc.replaceAll("\"", "").isEmpty()) {
						conceptPathSB.append(desc.replaceAll("\"", ""));
					
						conceptPathSB.append(PATH_SEPARATOR);
					} 
					
					mapping.setKey(file.getName() + ":" + x);
										
					mapping.setRootNode(conceptPathSB.toString());
					
					mapping.setDataType("TEXT");
					*/
				}
			}
			
			if(mapping.getRootNode().split(PATH_SEPARATOR.toString()).length > 2)  {
			
				buffer.write(mapping.toCSV() + "\n");
				buffer.flush();
			
			}
			x++;
		}
		
	}
	
	private static String findVariableDescription(String header, Document dictionary) {
		
		NodeList variables = dictionary.getElementsByTagName("variable");
		
		NodeList dataTable = dictionary.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String id = nodeMap.getNamedItem("id").getNodeValue(); //.replaceAll("\\..*", "");
		
		if(missingHeaders.get(id).contains(header)) return null;
		
		String name = "";
		
		String description = "";
		
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
	        			}
	        			
	        		}
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("description")) {
	        		
	        			description = node2.getTextContent();	
	        			
	        			if(!name.equalsIgnoreCase(header)) {
	        				description = "";
	        			}
	        			
	        		}
	        		
	        		if(!name.isEmpty() && !description.isEmpty() && name.equalsIgnoreCase(header)) break;	        		
	    		}
	    		if(!name.isEmpty() && !description.isEmpty() && name.equalsIgnoreCase(header)) return description;
		}
		if(description.isEmpty()) return header;
		
		return description;
		
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
		List<String> missing = new ArrayList();

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
	
	private static String[] getDataHeaders(File file) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()), StandardCharsets.ISO_8859_1)) {
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] record = line.split(new Character('\t').toString());
				
				if(record[0] == null) continue;
				if(record[0].startsWith("#")) continue;
				if(record[0].toLowerCase().contains("dbgap_subject")) return record;
				
			}
		} catch (IOException e) {
			if(e instanceof MalformedInputException) {
				System.err.println("Error with malformed file, needs further examination: " + file.getName());
				
				return null;
			} else {
				throw e;
			}
		}
		return null;
	}
	
	private static List<String> lookForDictionaries() throws ParserConfigurationException, SAXException, IOException {
		
		List<String> missingDictionaries = new ArrayList<>();
		
		if(Files.isDirectory(Paths.get(DATA_DIR))) {
			
			File[] dataFiles = new File(DATA_DIR).listFiles();

			// Iterate over data files and look for data dictionaries.
			Document dataDic = null;
			
			String pht = "";
			
			for(File file: dataFiles) {
				
				if(!file.getName().contains("phs")) continue;
				
				pht = getPht(file);
				if(pht == null) continue;
				if(!checkForDictionary(pht)) {
					
					missingDictionaries.add(file.getName());
										
				}
				
			}
			
		}
		
		return missingDictionaries;
		
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
	
	private static boolean checkForDictionary(String pht) {

		if(Files.isDirectory(Paths.get(DICT_DIR))) {
		
			for(File file: new File(DICT_DIR).listFiles()) {
				
				if(file.getName().contains(pht)) {
					
					return true;
					
				}
				
			}
			
		}
		
		return false;

	}
	
	private static void setLocalVariables(String[] args) throws NumberFormatException, Exception {
		/**
		 *  Flags override all settings
		 */
		for(String arg: args) {
			
			if(arg.equalsIgnoreCase( "-encodedlabel" )){
				String isencoded = checkPassedArgs(arg, args);
				if(isencoded.equalsIgnoreCase("Y")) {
					LABEL_IS_ENCODED = true;
				} 
			} 
			
		}		
	}

	private static String findDescription(Document dataDic) {
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();
		
		String desc = "";
		//if(USE_DESC == false) return desc;
		for(int x = 0 ; x < dataNodes.getLength() ; x++ ) {
			
			Node n = dataNodes.item(x);
			
			if(n.getNodeName().equals("description")) {
				
				desc = n.getTextContent();
				
				if(desc == null) return desc;
				
				desc = desc.replace("\"", "'");
				
				desc = desc.replace("\\", "");
			
			}
		}

		return desc;
	}
}
