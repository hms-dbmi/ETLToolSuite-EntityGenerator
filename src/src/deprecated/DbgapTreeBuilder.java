package src.deprecated;

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

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

public class DbgapTreeBuilder extends Job {

	private static final boolean USE_DESC = false;

	private static final String HIERARCHY_DIR = "./hierarchies/";

	private static boolean LABEL_IS_ENCODED = false;
	
	private static List<Integer> ignorecols = new ArrayList<Integer>();
	
	private static Set<Mapping> mappings = new HashSet<Mapping>();
	
	private static Set<Mapping> badMappings = new HashSet<Mapping>();
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			setClassVariables(args);
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
		
		PATH_SEPARATOR = "\\";
		
		boolean patientMappingWasGenerated = false;
		
		PatientMapping patientMapping = new PatientMapping();
		
		if(Files.isDirectory(Paths.get(DATA_DIR))) {
			
			File[] dataFiles = new File(DATA_DIR).listFiles();
			// Sample id to dbgap id mapping
			Map<String, String> dbgapidmap = new HashMap<String, String>();
			// build mapping file
			for(File data: dataFiles) {
				
				if(data.getName().toLowerCase().contains("subject.multi")) {
					buildDbgaidMap(data,dbgapidmap);
				}
				
			}
			
			for(File data: dataFiles) {
				
				// file description in xml if.
				// this will be a subfolder under root if it has a value.
				String fileDesc = "";
				
				Document dataDic = null;
				
				// hierarchy map < variable name, readable name >
				Map<String, Map<String, String>> hierMap = new HashMap<String,Map<String, String>>();
				
				try {
					
					String[] fileNameArr = data.getName().split("\\.");
					
					if(fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("xml")) continue;
					
					dataDic = readDataDict(fileNameArr,data.getName());
					
					hierMap = buildHierMap(fileNameArr[0]);
					
				} catch (ParserConfigurationException | SAXException | IOException e) {
					
					System.err.println(e);
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//if(dataDic != null) {
					
					fileDesc = findDescription(dataDic);
					
					if(fileDesc == null || fileDesc.isEmpty()) {
						fileDesc = findDescInFileName(data);
					}
					
					Map<String,String> headerLookup = new HashMap<String,String>();
					
					Map<String,String> phvLookup = new HashMap<String,String>();
					
					buildHeaderLookup(dataDic,headerLookup,phvLookup);
										
					Map<Integer,Map<String,String>> valueLookup = buildValueLookup(dataDic);
										
					try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + data.getName()),StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
						
						try(BufferedReader buffer = Files.newBufferedReader(Paths.get(data.getAbsolutePath()))){
							
							CSVReader reader = new CSVReader(buffer,',', '\"');
							
							String[] line;
							
							int headerLength = 0;
							
							String[] headers = new String[0];
							
							String[] rootNodes = new String[0];
							
							boolean headerWritten = false;
							
							boolean isSampleFile = false;
							
							while((line = reader.readNext()) != null) {
								boolean IS_BAD_MAPPING = false;
								//skip empty lines
								if(line[0].isEmpty()) continue; 
								// skip the comments
								if(line[0].charAt(0) == '#') continue;
								
								else if(!headerWritten) {
									
									if(line[0].toLowerCase().equals("dbgap_subject_id")) {
									
										isSampleFile = true;
										
									}
									
									headers = new String[line.length];
									
									rootNodes = new String[line.length];
									
									int idx = 0;
									
									for(String colheader: line) {
										/*
										if(data.getName().toLowerCase().contains("subject.multi")) {
											
											if(colheader.toLowerCase().contains("dbgap")) {
												
												patientMapping.setPatientKey(data.getName() + ':' + idx);
												
												patientMapping.setPatientColumn("patientnum");
												
												try(BufferedWriter patientWriter = Files.newBufferedWriter(Paths.get(PATIENT_MAPPING_FILE), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
													
													patientWriter.write(patientMapping.writeHeader());
													
													patientWriter.write(patientMapping.toCsv());
													
													patientWriter.flush();
													
													patientWriter.close();
													
												}
												
											}
											
										}
										*/
										if(headerLookup.containsKey(colheader)) {
											// replace all backslashes to forward slash and double quotes to single quotes
											String newheader = headerLookup.get(colheader);
											
											newheader = colheader.replace("\\", "/");
											
											newheader = colheader.replace("\"", "'");
					
											headers[idx] = newheader;
											
										} else {
									
											System.err.println("Header value: " + colheader + " does not exist in the data dictionary for " + data.getName());
											
											String newheader = colheader.replace("\\", "/");
											
											newheader = colheader.replace("\"", "'");
				
											headers[idx] = newheader;
										
										}
										if(phvLookup.containsKey(colheader)) {
											String phv = phvLookup.get(colheader);
											
											if(hierMap.containsKey(phv)) {
												
												String rootNode = hierMap.get(phv).get(colheader);
												
												rootNodes[idx] = rootNode.replaceAll("\"", "").trim();
												
											} else {
												rootNodes[idx] = null; //headers[idx];
												
											}
										} else {
											rootNodes[idx] = null; //colheader;
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
												
												if(colvalue.contains("\"") || colvalue.contains("\\")) {
													
													System.out.println("removing quotes and backslashes from: " + colvalue );
													
												}		
												
												colvalue = colvalue.replace("\\", "/");
												
												colvalue = colvalue.replace("\"", "");
												
												lineToWrite[idx] = colvalue;
											
											}
										}
										
										Mapping mapping = new Mapping();
 
										mapping.setKey(data.getName() + ':' + idx );
										
										if(rootNodes.length - 1 < idx || rootNodes[idx] == null) {
											idx++;
											continue;
										}
										
										if(fileDesc == null || fileDesc.isEmpty()) {
											
											mapping.setRootNode((rootNodes[idx] + PATH_SEPARATOR + headerLookup.get(headers[idx]) + PATH_SEPARATOR).replace("\\\\", "\\") );
										
										} else {
											if(LABEL_IS_ENCODED) {
												
												fileDesc = findCodedDesc(dataDic);
												
												fileDesc = fileDesc.replace("\\", "/");
												
												fileDesc = fileDesc.replace("\"", "");
												// if label is too long or does not exist tag as bad mapping that will need to be fixed.
												if(fileDesc == null || fileDesc.isEmpty() || fileDesc.length() > 50) {

													IS_BAD_MAPPING = true;
													
												}
												
											}
											if(headerLookup.containsKey(headers[idx])) {
												String rootNodePreclean = rootNodes[idx];
												
												ArrayList<String> pathNodes = new ArrayList<String>(Arrays.asList(rootNodePreclean.split("\\\\")));

												StringBuilder sb = new StringBuilder();
												
												if(LABEL_IS_ENCODED) {
													
													pathNodes.remove(pathNodes.size() - 1);
													
													pathNodes.add(pathNodes.size(), fileDesc);
													
													pathNodes.add(pathNodes.size(), headerLookup.get(headers[idx]));											
													
													for(String pnode: pathNodes) {
														sb.append(pnode.trim());
														sb.append('\\');
													}
													
												} else {
													
													pathNodes.add(pathNodes.size(), headerLookup.get(headers[idx]));											
													
													for(String pnode: pathNodes) {
														sb.append(pnode.trim());
														sb.append('\\');
													}
												
												}
												
												mapping.setRootNode(sb.toString().replace("\\\\", "\\").replaceAll("\"", "").trim());

											} else {
												String rootNodePreclean = rootNodes[idx];
												
												ArrayList<String> pathNodes = new ArrayList<String>(Arrays.asList(rootNodePreclean.split("\\\\")));

												StringBuilder sb = new StringBuilder();	
											
												for(String pnode: pathNodes) {
													sb.append(pnode.trim());
													sb.append('\\');
												}
												mapping.setRootNode(sb.toString().replace("\\\\", "\\").replaceAll("\"", "").trim());
												
												IS_BAD_MAPPING = true;
												
											}
										}
										
										//if(ignorecols.contains(idx)) {
											
											// omitted from the load
										//	mapping.setDataType("OMIT");
											
										//	mapping.setOptions("OMITTED BECAUSE VALUE IS ENCODED AND DOES NOT HAVE A DICTIONARY ENTRY");
											
										//} else {
											
										mapping.setDataType("TEXT");
											
										//}
										
										if(!mapping.getDataType().equalsIgnoreCase("omit") || !IS_BAD_MAPPING) {
											String rootNode = mapping.getRootNode();
											rootNode.replaceAll("\"", "");
											if(rootNode.charAt(0) != PATH_SEPARATOR.charAt(0)) mapping.setRootNode(PATH_SEPARATOR + rootNode);
											if(rootNode.charAt(rootNode.length() - 1) != PATH_SEPARATOR.charAt(0)) mapping.setRootNode(PATH_SEPARATOR + rootNode);
											mapping.setRootNode(rootNode);
											mappings.add(mapping);
										} else {
											
											String rootNode = mapping.getRootNode();
											rootNode.replace('"', ' ');
											if(rootNode.charAt(0) != PATH_SEPARATOR.charAt(0)) mapping.setRootNode(PATH_SEPARATOR + rootNode);
											if(rootNode.charAt(rootNode.length() - 1) != PATH_SEPARATOR.charAt(0)) mapping.setRootNode(PATH_SEPARATOR + rootNode);
											mapping.setRootNode(rootNode);
											badMappings.add(mapping);
										}
										
										idx++;
										
									}
									if(isSampleFile) {
										
										
										if(!lineToWrite[0].toLowerCase().contains("dbgap_subject_id")) {
											
											if(dbgapidmap.containsKey(lineToWrite[0])) {
												
												lineToWrite[0] = dbgapidmap.get(lineToWrite[0]);
												
											} else {
												
												System.err.println("Sample id " + line[0] + " does not have a corresponding dbgap subject id.");
												
											}
											
										}
										
									}
									
									writer.write(toCsv(lineToWrite));

								}
								
							}
							
						}

					}
					
				//} else {
					
					//System.err.println("File: " + data.getName() + " Does not have a corresponding dictionary file in " + DICT_DIR);
					
				//}
				
			}
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(MAPPING_FILE), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
				
				List<Mapping> sorted = new ArrayList<Mapping>(mappings);
				
				Collections.sort(sorted);
				
				writer.write(Mapping.printHeader() + '\n');
				
				for(Mapping mapping: sorted) {
					
					writer.write(mapping.toCSV() + '\n');
					
				}
				
			}
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(MAPPING_DIR + "bad_mappings.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
				
				List<Mapping> sorted = new ArrayList<Mapping>(badMappings);
				
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




	private static void buildDbgaidMap(File data, Map<String, String> dbgapidmap) throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(data.getAbsolutePath()))){
			CSVReader reader = new CSVReader(buffer,'\t', '∂');	
			
			String[] line;
			boolean firstColIsDbgapSubj = false;

			while((line = reader.readNext()) != null ) {
				//skip empty lines
				if(line[0].isEmpty()) continue; 
				// skip the comments
				if(line[0].charAt(0) == '#') continue;
				
				
				if(line[0].toLowerCase().contains("subject")) {
					firstColIsDbgapSubj = true;
					continue;
				}
				
				if(firstColIsDbgapSubj) {
					
					dbgapidmap.put(line[1], line[0]);
					
				}
				
			}
			
		}
		
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




	private static void buildHeaderLookup(Document dataDic, Map<String,String> headerLookup, Map<String,String> phvLookup) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		String dictPhenoId = nodeMap.getNamedItem("id").getNodeValue();
		
		String studyId = nodeMap.getNamedItem("study_id").getNodeValue();
		
		String participantSet = nodeMap.getNamedItem("participant_set").getNodeValue();
		
		String dateCreated = nodeMap.getNamedItem("date_created").getNodeValue();
		
		// build an object that will collect the variables
		
		NodeList variables = dataDic.getElementsByTagName("variable");
				
	    for (int idx = 0; idx < variables.getLength(); idx++) {
	
	    		Node node = variables.item(idx);
	    		
	    		NodeList variableChildren = node.getChildNodes();
	    		
	    		String id = node.getAttributes().getNamedItem("id").getNodeValue().split("\\.")[0];
	    		
	    		String name = "";
	    		
	    		String desc = "";
	    		
	    		for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
	
	        		Node node2 = variableChildren.item(idx2);
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("name")) {
	        			name = node2.getTextContent();

	        			phvLookup.put(name, id);
	        		}
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("description")) desc = node2.getTextContent().replace('\"', '\'');
	        		
	    		}
	    		
	    		headerLookup.put(name, desc);
	    		
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
	    				ignorecols.add(idx);

	    				continue;
	    				
	    			} else {
	    				
	    				valueCoded = vnode.getAttributes().getNamedItem("code").getNodeValue();
	    				
	    			}
	    			
	    			valueMap.put(valueCoded, valueDecoded);
	    		}
	    		valueLookup.put(idx + 1, valueMap);
	    }
	    
	    return valueLookup;
	    
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




	private static String findCodedDesc(Document dataDic) {
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();

		Node node = dataNodes.item(0);
		
		String desc = node.getTextContent();
		return desc;
	}

	private static String findDescription(Document dataDic) {
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();
		
		String desc = "";
		if(USE_DESC == false) return desc;
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
				if(!fileNameArr[fileNameArr.length - 2].equalsIgnoreCase("data_dict")) continue;
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

	private static Object findPHV(Document dataDic) {
		// TODO Auto-generated method stub
		return null;
	}

	private static void setClassVariables(String[] args) throws NumberFormatException, Exception {
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

}
