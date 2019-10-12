package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class DbgapTreeBuilder extends Job {

	private static List<Integer> ignorecols = new ArrayList<Integer>();
	
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
			
		}		
	}




	private static void execute() throws IOException {

		if(Files.isDirectory(Paths.get(DATA_DIR))) {
			
			File[] dataFiles = new File(DATA_DIR).listFiles();
			
			Set<Mapping> mappings = new HashSet<Mapping>();

			for(File data: dataFiles) {
				// file description in xml if.
				// this will be a subfolder under root if it has a value.
				String fileDesc = "";
				
				Document dataDic = null;
				try {
					
					String[] fileNameArr = data.getName().split("\\.");
					
					if(fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("xml")) continue;
					
					dataDic = readDataDict(fileNameArr,data.getName());
				
				} catch (ParserConfigurationException | SAXException | IOException e) {
					
					System.err.println(e);
				
				}
				
				if(dataDic != null) {
					
					fileDesc = findDescription(dataDic);
					
					if(fileDesc == null || fileDesc.isEmpty()) {
						fileDesc = findDescInFileName(data);
					}
					
					Map<String,String> headerLookup = buildHeaderLookup(dataDic);
					
					Map<Integer,Map<String,String>> valueLookup = buildValueLookup(dataDic);
										
					try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + data.getName()),StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
						
						try(BufferedReader buffer = Files.newBufferedReader(Paths.get(data.getAbsolutePath()))){
							
							CSVReader reader = new CSVReader(buffer,'\t', '∂');
							
							String[] line;
							
							int headerLength = 0;
							
							String[] headers = new String[0];
							
							boolean headerWritten = false;
							
							while((line = reader.readNext()) != null) {
								//skip empty lines
								if(line[line.length - 1].isEmpty()) continue; 
								// skip the comments
								if(line[0].charAt(0) == '#') continue;
								
								else if(!headerWritten) {
																
									headers = new String[line.length];
									
									int idx = 0;
									
									for(String colheader: line) {
							
										if(headerLookup.containsKey(colheader)) {
											// replace all backslashes to forward slash and double quotes to single quotes
											colheader = headerLookup.get(colheader);
											
											colheader = colheader.replace("\\", "/");
											
											colheader = colheader.replace("\"", "'");
					
											headers[idx] = colheader;
										} else {
									
											System.err.println("Header value: " + colheader + " does not exist in the data dictionary for " + data.getName());
											
											colheader = colheader.replace("\\", "/");
											
											colheader = colheader.replace("\"", "'");
				
											headers[idx] = colheader;
										
										}
										
										headerLength = line.length;
										
										idx++;
										
									}
									
									writer.write(toCsv(headers));
									
									headerWritten = true;

								} else {
									
									int idx = 0;
									
									String[] lineToWrite = new String[line.length];
																		
									for(String colvalue: line) {
										
										if(valueLookup.containsKey(idx)) {
											// replace all backslashes to forward slash and double quotes to single quotes
											Map<String,String> vlookup = valueLookup.get(idx);
											
											if(vlookup.containsKey(colvalue)) {
											
												colvalue = vlookup.get(colvalue);

												colvalue = colvalue.replace("\\", "/");
											
												colvalue = colvalue.replace("\"", "'");
											
												lineToWrite[idx] = colvalue;
									
											} else {
												if(colvalue.contains("\"")) {
													
													System.out.println();
													
												}									
												colvalue = colvalue.replace("\\", "/");
												
												colvalue = colvalue.replace("\"", "'");
												
												lineToWrite[idx] = colvalue;
											
											}
										}
										
										Mapping mapping = new Mapping();
 
										mapping.setKey(data.getName() + ':' + idx );
										
										if(fileDesc == null || fileDesc.isEmpty()) {
										
											mapping.setRootNode(ROOT_NODE + headers[idx] + PATH_SEPARATOR);
										
										} else {
										
											fileDesc = fileDesc.replace("\\", "/");
											
											fileDesc = fileDesc.replace("\"", "'");

											mapping.setRootNode(ROOT_NODE + fileDesc + PATH_SEPARATOR + headers[idx] + PATH_SEPARATOR);
																					
										}
										
										if(ignorecols.contains(idx)) {
											
											// omitted from the load
											mapping.setDataType("OMIT");
											
											mapping.setOptions("OMITTED BECAUSE VALUE IS ENCODED AND DOES NOT HAVE A DICTIONARY ENTRY");
											
										} else {
											
											mapping.setDataType("TEXT");
											
										}
										
										mappings.add(mapping);
										
										idx++;
										
									}

									writer.write(toCsv(lineToWrite));

								}
							}
							
						}

					}
				} else {
					
					//System.err.println("File: " + data.getName() + " Does not have a corresponding dictionary file in " + DICT_DIR);
					
				}
				
			}
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(MAPPING_FILE), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
				
				List<Mapping> sorted = new ArrayList<Mapping>(mappings);
				
				Collections.sort(sorted);
				
				writer.write(Mapping.printHeader() + '\n');
				
				for(Mapping mapping: sorted) {
					
					writer.write(mapping.toCSV() + '\n');
					
				}
				
			}
			
		} else {
			
			System.err.println("DATA_DIR " + DATA_DIR + " IS NOT A DIRECTORY!");
			throw new IOException();
		}		
	}




	private static Map<Integer, Map<String, String>> buildValueLookup(Document dataDic) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String dictPhenoId = nodeMap.getNamedItem("id").getNodeValue();
		
		String studyId = nodeMap.getNamedItem("study_id").getNodeValue();
		
		String participantSet = nodeMap.getNamedItem("participant_set").getNodeValue();
		
		String dateCreated = nodeMap.getNamedItem("date_created").getNodeValue();
		
		// build an object that will collect the variables
		
		NodeList variables = dataDic.getElementsByTagName("variable");
		
		// Colheader<List<Map<encodedvalue, decodedvalue>>>
		Map<Integer, Map<String, String>> valueLookup = new HashMap<Integer, Map<String, String>>();
		
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
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("type")) type = node2.getTextContent();
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("value")) valueNodes.add(node2);
        		}
        		
        		Map<String,String> valueMap = new HashMap<String,String>(); 
        		
        		if(!type.equalsIgnoreCase("encoded")) continue;
        		
        		for(Node vnode: valueNodes) {
        			
        			Map<String,String> vmap = new HashMap<String,String>(); 
        			
        			String valueDecoded = vnode.getFirstChild().getNodeValue();
        			
        			String valueCoded;
        			
        			if(vnode.getAttributes().getNamedItem("code") == null) {
        				// ignore variable as it is missing its decoded value
        				// dictionary needs to be updated.
        				ignorecols.add(idx);
        				
        				continue;
        				
        			} else {
        				
        				valueCoded = vnode.getAttributes().getNamedItem("code").getNodeValue();
        				
        			}
        			
        			valueMap.put(valueCoded, valueDecoded);
        		}
        		valueLookup.put(idx, valueMap);
        }
        
        return valueLookup;
        
	}

	private static Map<String, String> buildHeaderLookup(Document dataDic) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String dictPhenoId = nodeMap.getNamedItem("id").getNodeValue();
		
		String studyId = nodeMap.getNamedItem("study_id").getNodeValue();
		
		String participantSet = nodeMap.getNamedItem("participant_set").getNodeValue();
		
		String dateCreated = nodeMap.getNamedItem("date_created").getNodeValue();
		
		// build an object that will collect the variables
		
		NodeList variables = dataDic.getElementsByTagName("variable");
		
		Map<String,String> headerLookup = new HashMap<String,String>();
		
        for (int idx = 0; idx < variables.getLength(); idx++) {

        		Node node = variables.item(idx);
        		
        		NodeList variableChildren = node.getChildNodes();
        		
        		String name = "";
        		
        		String desc = "";
        		
        		for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {

	        		Node node2 = variableChildren.item(idx2);
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("name")) name = node2.getTextContent();
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("description")) desc = node2.getTextContent();
	        		
        		}
        		
        		headerLookup.put(name, desc);
        		
        }
		
		return headerLookup;
	}




	private static Document readDataDict(String[] fileNameArr, String name) throws ParserConfigurationException, SAXException, IOException {

		if(fileNameArr.length > 3) { 
		
			String accession = fileNameArr[0]; 
		
			String version = fileNameArr[1];
		
			String phenotype = fileNameArr[2];
					
			Document doc = findDictionary(accession,version,phenotype);
		
			return doc;
			
		} else {
			
			System.err.println("File: " + name + " invalid dbgap format expecting ( phsXXXXXX.vX.phtXXXXXX )");
			
			return null;
			
		}
	}




	private static String findDescInFileName(File data) {
		String fileName = data.getName();
		
		String[] parts = fileName.split("\\.");
				
		String desc = parts[6];
		
		desc = desc.replaceAll(String.format("%s|%s|%s",
	     "(?<=[A-Z])(?=[A-Z][a-z])",
	     "(?<=[^A-Z])(?=[A-Z])",
	     "(?<=[A-Za-z])(?=[^A-Za-z])")," ");
		
		desc = desc.replace("_", "");
		
		desc = desc.replaceAll(" +", " ");
		
		desc = desc.replace("\"", "'");
		desc = desc.replace("\\", "");

		return desc;
	}




	private static String findDescription(Document dataDic) {
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();
		
		String desc = "";
		
		for(int x = 0 ; x < dataNodes.getLength() ; x++ ) {
			
			Node n = dataNodes.item(x);
			
			if(n.getNodeName().equals("description")) {
				
				desc = n.getNodeValue();
				
				if(desc == null) return desc;
				
				desc = desc.replace("\"", "'");
				desc = desc.replace("\\", "");
			}
		}
		


		return desc;
	}




	private static Document findDictionary(String accession, String version, String phenotype) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		
		if(Files.isDirectory(Paths.get(DICT_DIR))) {
			
			File dictFile = null;
			
			for(File f: new File(DICT_DIR).listFiles()) {
				
				if(f.isDirectory()) continue;
				
				String[] fileNameArr = f.getName().split("\\.");
				
				if(!fileNameArr[0].equals(accession)) continue;
				
				if(!fileNameArr[1].equals(version)) continue;
				
				if(!fileNameArr[2].equals(phenotype)) continue;
				
				if(!fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("xml")) continue;
				else {
					dictFile = f;
					break;
				}
			}
			if(dictFile == null) return null;
			return builder.parse(dictFile);
		} else {
			System.err.println("DICT_DIR " + DICT_DIR + " IS NOT A DIRECTORY!");
			throw new IOException();
		}
	}




	private static char[] toCsv(String[] line) {
		StringBuilder sb = new StringBuilder();
		
		int lastNode = line.length - 1;
		int x = 0;
		for(String node: line) {
			
			sb.append('"');
			sb.append(node);
			sb.append('"');
			
			if(x == lastNode) {
				sb.append('\n');
			} else {
				sb.append(',');
			}
			x++;
		}
		
		return sb.toString().toCharArray();
	}

}
