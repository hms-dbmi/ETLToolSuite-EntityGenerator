package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.opencsv.CSVReader;

import etl.job.entity.Mapping;

public class DbgapTreeBuilder2 extends Job {

	private static final String HIERARCHY_DIR = "./hierarchies/";
	private static final boolean USE_DESC = false;
	private static boolean PROCESS_MISSING_DICTIONARY = false;
	private static boolean LABEL_IS_ENCODED = false;
	private static List<String> missingDictionaries;
	private static Map<String, List<String>> missingHeaders;
	
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
			
		}	
	}
	private static void execute() throws IOException, ParserConfigurationException, SAXException {
		// gather data files that do not have dictionaries
		// files missing dictionaries will be reported out. 
		// 
		missingDictionaries = lookForDictionaries();
		// Build the mapping file.
		buildMappingFile();
		
	}
	
	private static void buildMappingFile() throws ParserConfigurationException, SAXException, IOException {
		
		File dir = new File(DATA_DIR);
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(MAPPING_FILE), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			if(dir.isDirectory()) {
				for(File file: dir.listFiles()) {
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
		String str = ROOT_NODE;
		
		String desc = findDescription(dictionary);
		int x = 0;
		
		List<Mapping> mappings = new ArrayList<>();
		
		for(String header: headers) {
			Mapping mapping = new Mapping();
			if(!missingHeaders.containsKey(header)) {
				
				String varDesc = findVariableDescription(header,dictionary);
				
				if(varDesc != null) {
				
					StringBuilder conceptPathSB = new StringBuilder();
						
					conceptPathSB.append(ROOT_NODE);
										
					conceptPathSB.append(desc);
					
					conceptPathSB.append(PATH_SEPARATOR);
					
					conceptPathSB.append(varDesc);

					conceptPathSB.append(PATH_SEPARATOR);
					
					mapping.setKey(file.getName() + ":" + x);
					
					mapping.setRootNode(conceptPathSB.toString());
					
					mapping.setDataType("TEXT");
					
				} else {
					
					StringBuilder conceptPathSB = new StringBuilder();
					
					conceptPathSB.append(PATH_SEPARATOR);
	
					conceptPathSB.append(ROOT_NODE);
					
					conceptPathSB.append(PATH_SEPARATOR);
					
					conceptPathSB.append(desc);
					
					conceptPathSB.append(PATH_SEPARATOR);
					
					mapping.setKey(file.getName() + ":" + x);
										
					mapping.setRootNode(conceptPathSB.toString());
					
					mapping.setDataType("TEXT");

				}
			}
			buffer.write(mapping.toCSV() + "\n");
			buffer.flush();
			x++;
		}
		
	}
	
	private static String findVariableDescription(String header, Document dictionary) {
		NodeList variables = dictionary.getElementsByTagName("variable");
		
		NodeList dataTable = dictionary.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String id = nodeMap.getNamedItem("id").getNodeValue(); //.replaceAll("\\..*", "");
		
		if(missingHeaders.get(id).contains(header)) return header;
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
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] record = line.split(new Character('\t').toString());
				
				if(record[0] == null) continue;
				if(record[0].startsWith("#")) continue;
				if(record[0].toLowerCase().contains("dbgap_subject")) return record;
				
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
				
				if(!checkForDictionary(pht)) {
					
					missingDictionaries.add(file.getName());
										
				}
				
			}
			
		}
		
		return missingDictionaries;
		
	}
	
	private static String getPht(File file) {
		System.out.println(file.getName());
		return file.getName().split("\\.")[2];
	}
	private static Document getDictionary(String pht) throws ParserConfigurationException, SAXException, IOException {
		
		File dir = new File(DICT_DIR);
		
		File[] files = dir.listFiles(new FileFilter() {

		    @Override
		    public boolean accept(File file) {
		    	
		        return file.getName().contains(pht);
		        
		    }
		
		});
		
		if(files.length == 1) {
			
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			
			return builder.parse(files[0]);
			
		} else {
		
			return null;
		
		}
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
	

	private static NodeList buildValueLookup(Document dataDic) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String dictPhenoId = nodeMap.getNamedItem("id").getNodeValue();
		
		String studyId = nodeMap.getNamedItem("study_id").getNodeValue();
		
		String participantSet = nodeMap.getNamedItem("participant_set").getNodeValue();
		
		String dateCreated = nodeMap.getNamedItem("date_created").getNodeValue();
		
		// build an object that will collect the variables
		
		return dataDic.getElementsByTagName("variable");
		/*
		// Colheader<List<Map<encodedvalue, decodedvalue>>>
		Map<Integer, Map<String, String>> valueLookup = new HashMap<Integer, Map<String, String>>();
		
		valueLookup.put(0,new HashMap<String,String>());
		valueLookup.put(1,new HashMap<String,String>());
		
	    for (int idx = 0; idx < variables.getLength(); idx++) {
	
	    		Node node = variables.item(idx);
	    		
	    		NodeList variableChildren = node.getChildNodes();
	    		
	    		String name = "";
	    		
	    		String desc = "";
	    		
	    		String type = "";
	    		
	    		List<Node> valueNodes = new ArrayList<Node>();
	    		
	    		for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
	
	        		Node node2 = variableChildren.item(idx2);
	        			        		
	        		if(node2.getNodeName().equalsIgnoreCase("name")) name = node2.getTextContent();
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("description")) desc = node2.getTextContent();
	        		
	        		//if(node2.getNodeName().equalsIgnoreCase("type")) type = node2.getTextContent();
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("value")) valueNodes.add(node2);
	    		}
	    		
	    		Map<String,String> valueMap = new HashMap<String,String>(); 
	    		
	    		//if(!type.equalsIgnoreCase("encoded")) continue;
	    		
	    		for(Node vnode: valueNodes) {
	    			
	    			Map<String,String> vmap = new HashMap<String,String>(); 
	    			
	    			String valueDecoded = vnode.getFirstChild().getNodeValue();
	    			
	    			String valueCoded;
	    			
	    			if(vnode.getAttributes().getNamedItem("code") == null) {
	    				// ignore variable as it is missing its decoded value
	    				// dictionary needs to be updated.
	    				//ignorecols.add(idx);

	    				continue;
	    				
	    			} else {
	    				
	    				valueCoded = vnode.getAttributes().getNamedItem("code").getNodeValue();
	    				
	    			}
	    			
	    			valueMap.put(valueCoded, valueDecoded);
	    		}
	    		valueLookup.put(idx + 1, valueMap);
	    }
	    
	    return valueLookup;
	   */ 
	}

	private static HashMap<String,Map<String, String>> buildHierMap(String phs) throws Exception {
		HashMap<String,Map<String, String>> hierMap = new HashMap<String,Map<String,String>>();
		
		File hierdir = new File(HIERARCHY_DIR);
		if(hierdir.isDirectory()) {
			for(File file: hierdir.listFiles()) {
				if(file.getName().contains(phs) && file.getName().contains("hierarchy")) {
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))){
						CSVReader reader = new CSVReader(buffer,',','"','√');
						
						String[] line;
						while((line = reader.readNext() )!= null) {
							Map<String, String> map = new HashMap<String,String>();
							
							map.put(line[2], line[0]);
							hierMap.put(line[1], map);
						}
					}
					break;
				}
			}
		}
		if(hierMap.isEmpty()) System.err.println("No hierarchy found for: " + phs);
		return hierMap;

		
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
