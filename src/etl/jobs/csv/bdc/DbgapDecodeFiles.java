package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.File;
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

import etl.jobs.Job;

public class DbgapDecodeFiles extends Job {

	public static void main(String[] args) {
		try {
			
			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}
		
		try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws ParserConfigurationException, SAXException, IOException {
		// iterate over all files in data dir.  
		// look for data files and get the pht value;
		
		if(Files.isDirectory(Paths.get(DATA_DIR))) {

			File[] dataFiles = new File(DATA_DIR).listFiles();

			for(File data: dataFiles) {

				String[] fileNameArr = data.getName().split("\\.");
				//
				if(!fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("txt")) continue;
				
				String pht = getPht(fileNameArr);
				
				// if filenames does not contain pht continue to next file.
				if(pht == null || pht.isEmpty()) continue;
				
				// get Data Dictionary
				File dictionaryFile = getDictFile(pht);
				
				translateData(data, dictionaryFile);
			}
			
		}
		
	}
	/**
	 * 
	 * Take the data and dictionary file 
	 * stream a writer to a temp file.
	 * once file is complete overwrite data file with temp file
	 * 
	 * @param data
	 * @param dictionaryFile
	 */
	private static void translateData(File data, File dictionaryFile) {
		
		Document dataDic = buildDictionary(dictionaryFile);

		Map<String,String> headerLookup = new HashMap<>();
		 
		Map<String,String> phvLookup = new HashMap<>();
		
		Map<String, Map<String, String>> valueLookup = (dataDic == null) ? null: buildValueLookup(dataDic);

		if(dataDic != null) buildHeaderLookup(dataDic, headerLookup, phvLookup);

		// Start a writer stream to output translated data file
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + data.getName()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			// Stream data file and output buffer
			
			CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(data.getAbsolutePath())), '\t','µ');
			
			String[] line;
			
			String[] headers = getHeaders(reader);
			
			if(headers != null) {
				
				String[] lineToWrite = new String[headers.length];

				while((line = reader.readNext()) != null) {
					int colidx = 0;
					for(String cell: line) {
						if(headers.length - 1 < colidx) continue;

						String header = headers[colidx];
						
						if(valueLookup != null && valueLookup.containsKey(header)) {
							
							Map<String,String> codedValues = valueLookup.get(header);

							if(codedValues.containsKey(cell)) {

								cell = codedValues.get(cell);	
								
							}
							
							
						}
						lineToWrite[colidx] = cell;
						colidx++;
					}
					buffer.write(toCsv(lineToWrite));
					buffer.flush();
				}
			} else {
				
				System.err.println("Missing headers for " + data.getName());
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error writing to " + WRITE_DIR + data.getName());
			e.printStackTrace();
		}
	
	}

	private static String[] getHeaders(CSVReader reader) throws IOException {

		String[] line;
		
		while((line = reader.readNext()) != null) {
			if(line[0].equalsIgnoreCase("dbgap_subject_id")) {
				return line;
			}
		}
		return null;
	}

	private static String[] getHeaders(File data) throws IOException {

		CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(data.getAbsolutePath())), '\t');

		String[] line;
		
		while((line = reader.readNext()) != null) {
			if(line[0].equalsIgnoreCase("dbgap_subject_id")) {
				return line;
			}
		}
		return null;
	}

	private static Document buildDictionary(File dictionaryFile) {
		if(dictionaryFile == null) return null;
		try {
	
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			
			
			DocumentBuilder builder;
				builder = factory.newDocumentBuilder();
					
			return builder.parse(dictionaryFile);
			
		} catch (ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error processing dictionary file " + dictionaryFile.getAbsolutePath());
			System.err.println(e.toString());
			e.printStackTrace();
		}
		return null;
	}

	private static File getDictFile(String pht) {
		// Looking for data dictionary filename
		if(Files.isDirectory(Paths.get(DATA_DIR))) {

			File[] dataFiles = new File(DATA_DIR).listFiles();
			
			for(File data: dataFiles) {
				if(!data.getName().contains("data_dict.xml")) continue;
				String[] fileNameArr = data.getName().split("\\.");
				// look for xml
				if(!fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("xml")) continue;
				// if pht matches return file
				if(pht.equalsIgnoreCase(getPht(fileNameArr))) return data;
				
			}
		}
		return null;
	}

	private static String getPht(String[] fileNameArr) {
		for(String str: fileNameArr) {
			if(str.contains("pht")) return str;
		}
		return null;
	}

	private static String findCodedDesc(Document dataDic) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		NodeList dataNodes = dataTable.item(0).getChildNodes();

		Node node = dataNodes.item(0);
		
		String desc = node.getTextContent();
		
		return desc;
		
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
	
	private static void buildHeaderLookup(Document dataDic, Map<String,String> headerLookup, Map<String,String> phvLookup) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();

		
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
	
	private static Map<String, Map<String, String>> buildValueLookup(Document dataDic) {
		
		NodeList dataTable = dataDic.getElementsByTagName("data_table");
		
		Node dataNode = dataTable.item(0);
		
		NamedNodeMap nodeMap = dataNode.getAttributes();
		
		// build an object that will collect the variables
		
		NodeList variables = dataDic.getElementsByTagName("variable");
		
		// Colheader<List<Map<encodedvalue, decodedvalue>>>
		Map<String, Map<String, String>> valueLookup = new HashMap<String, Map<String, String>>();
		
		//valueLookup.put(0,new HashMap<String,String>());
		//valueLookup.put(1,new HashMap<String,String>());
		
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
	    		valueLookup.put(name, valueMap);
	    }
	    
	    return valueLookup;
	    
	}
	
}
