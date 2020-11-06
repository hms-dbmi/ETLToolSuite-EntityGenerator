package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.Job;

public abstract class BDCJob extends Job {

	public static CSVReader readRawBDCDataset(Path fileName) throws IOException {
		return readRawBDCDataset(fileName,false);
	}
	
	public static CSVReader readRawBDCDataset(Path fileName, boolean removeComments) throws IOException {
		BufferedReader buffer = Files.newBufferedReader(fileName);
		CSVReader reader = new CSVReader(buffer, '\t', 'π');
		
		String[] headers;
		
		if(removeComments) {
			while((headers = reader.readNext()) != null) {

				boolean isComment = ( headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() ) ? true: headers[0].isEmpty() ? true: false;
				
				if(isComment) {
					
					continue;
					
				} else {
					
					break;
					
				}
				
			}
		} 
		
		return reader;
			
		
	}
	public static CSVReader readRawBDCDataset(File file, boolean removeComments) throws IOException {
		return readRawBDCDataset(Paths.get(file.getCanonicalPath()), removeComments);
	}
	
	public static CSVReader readRawBDCDataset(File file) throws IOException {
		return readRawBDCDataset(Paths.get(file.getCanonicalPath()), false);
	}

	public static int findRawDataColumnIdx(Path fileName, String columnName) throws IOException {
		if(fileName == null) return -1;
		BufferedReader buffer = Files.newBufferedReader(fileName);
		CSVReader reader = new CSVReader(buffer, '\t', 'π');
		
		String[] headers;
	
		while((headers = reader.readNext()) != null) {

			boolean isComment = ( headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() ) ? true: headers[0].isEmpty() ? true: false;
			
			if(isComment) {
				
				continue;
				
			} else {
				
				break;
				
			}
			
		}
		int x = 0;
		for(String header: headers) {
			
			if(header.toLowerCase().equals(columnName.toLowerCase())) {
				return x;
			}
			x++;
		}
		return -1;  // not found
			
	}
	
	public static File FindDictionaryFile(String fileName) {
		if(!fileName.startsWith("phs")) {
			System.err.println("Bad Filename - Expecting a valid accessions phsxxxxxx for: " + fileName);
		
		}
		
		File dataDir = new File(DATA_DIR);
		
		String phs = null;
		
		String pht = null;
		
		if(dataDir.isDirectory()) {
			
		
			String[] fileNameArr = fileName.split("\\.");
			
			phs = fileNameArr[0];
			
			final String finalPhs = phs;
			
			final String finalPht = getPht(fileName);
			
			File[] dataDicts = dataDir.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.contains(finalPhs) && name.contains(finalPht) && name.toLowerCase().contains("data_dict")) {
						return true;
					}
					return false;
				}
			});
			
			if(dataDicts.length < 1) {
				System.err.println("Data Dictionary missing for " + fileName);
				return null;
			} else {
				return dataDicts[0];
			}
		} else {
			System.err.println(DATA_DIR + " IS NOT A DIRECTORY!");
		}
		return null;
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
	
	public static String getPht(String dbGapFileName) {
		String[] fileNameArr = dbGapFileName.split("\\.");
		for(String str: fileNameArr) {
			if(str.contains("pht")) return str;
		}
		return null;
	}
	
	public static String getPht(String[] dbGapFileNameArr) {
		for(String str: dbGapFileNameArr) {
			if(str.contains("pht")) return str;
		}
		return null;
	}
	
	public static Document buildDictionary(File dictionaryFile) {
		if(dictionaryFile == null) return null;
		try {
	
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			
			DocumentBuilder builder = factory.newDocumentBuilder();
					
			return builder.parse(dictionaryFile);
			
		} catch (ParserConfigurationException | SAXException | IOException e) {
			
			System.err.println("Error processing dictionary file " + dictionaryFile.getAbsolutePath());
			
			System.err.println(e.toString());
			
			e.printStackTrace();
			
		}
		return null;
	}
	
	public static String getStudySubjectMultiFile(BDCManagedInput managedInput) throws IOException {
		File dataDir = new File(DATA_DIR);
		
		String studyIdentifier = managedInput.getStudyIdentifier();
		
		if(dataDir.isDirectory()) {
			
			String[] fileNames = dataDir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith(studyIdentifier) && name.toLowerCase().contains("subject.multi") && name.toLowerCase().endsWith(".txt")) {
						return true;
					} else {
						return false;
					}
				}
			});
	
			if(fileNames.length >= 1) return fileNames[0];
			else return null;
		} else {
			throw new IOException(dataDir + " is not a directory.  Check datadir parameter", new Throwable().fillInStackTrace());
		}
	}
	
	public static String getStudySampleMultiFile(BDCManagedInput managedInput) throws IOException {
		File dataDir = new File(DATA_DIR);
				
		String studyIdentifier = managedInput.getStudyIdentifier();
		
		if(dataDir.isDirectory()) {
			
			String[] fileNames = dataDir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith(studyIdentifier) && name.toLowerCase().contains("sample.multi") && name.toLowerCase().endsWith(".txt")) {
						return true;
					} else {
						return false;
					}
				}
			});
	
			if(fileNames.length == 1) return fileNames[0];
			else return null;
		} else {
			throw new IOException(dataDir + " is not a directory.  Check datadir parameter", new Throwable().fillInStackTrace());
		}		
	}
	
	protected static Map<String, Map<String, String>> buildValueLookup(Document dataDic) {
		
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
	public static List<String> getPatientSetForConsentFromRawData(String subjectFileName, BDCManagedInput managedInput, List<String> consentHeaders, String consent_group_code) throws IOException {
		List<String> patientSet = new ArrayList<>();
		if(!new File(DATA_DIR + subjectFileName).exists()) return patientSet;
		int cgcidx = -1;
		
		for(String header: consentHeaders) {
			
			cgcidx = findRawDataColumnIdx(Paths.get(DATA_DIR + subjectFileName), header); 
			if(cgcidx != -1) break;
		
		}

		int patientIdCol = findRawDataColumnIdx(Paths.get(DATA_DIR + subjectFileName), managedInput.getPhsSubjectIdColumn());
		
		if(patientIdCol == -1) {
			System.err.println("Error no subject id column found for " + managedInput.getStudyAbvName() + " - " + managedInput.getPhsSubjectIdColumn() +
					" in subject file for " + subjectFileName + " will try SUBJECT_ID");
			
			patientIdCol = findRawDataColumnIdx(Paths.get(DATA_DIR + subjectFileName), "SUBJECT_ID");
			
			if(patientIdCol == -1) {
				System.err.println("Error no subject id column found for " + managedInput.getStudyAbvName() + " - " + "SUBJECT_ID" +
						" in subject file for " + subjectFileName + " will try SUBJECT_ID");
				return patientSet;

			}
		}
		
		if(cgcidx != -1) {
			
			BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + subjectFileName));
			
			CSVReader reader = new CSVReader(buffer, '\t', 'π');
			
			String[] headers;
		
			while((headers = reader.readNext()) != null) {

				boolean isComment = ( headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() ) ? true: headers[0].isEmpty() ? true: false;
				
				if(isComment) {
					
					continue;
					
				} else {
					
					if(headers.length > cgcidx) {
						
						if(headers[cgcidx].equalsIgnoreCase(consent_group_code)) patientSet.add(headers[patientIdCol]);
						
					}
					
				}
				
			}
			
		}
		
		return patientSet;
	}
	public static List<String> getPatientSetFromSampleFile(String sampleFileName, BDCManagedInput managedInput) throws IOException {
		List<String> patientSet = new ArrayList<>();
		
		int patientIdCol = findRawDataColumnIdx(Paths.get(DATA_DIR + sampleFileName), managedInput.getPhsSubjectIdColumn()); 
		
		if(patientIdCol == -1) {
			System.err.println("Error no subject id column found for " + managedInput.getStudyAbvName() + " - " +
					managedInput.getPhsSubjectIdColumn() + " in sample multi file for " + sampleFileName
						);
			
			patientIdCol = findRawDataColumnIdx(Paths.get(DATA_DIR + sampleFileName), "SUBJECT_ID");
			
			if(patientIdCol == -1) {
				System.err.println("Error no subject id column found for " + managedInput.getStudyAbvName() + " - " + "SUBJECT_ID" +
						" in subject file for " + sampleFileName + " will try SUBJECT_ID");
				return patientSet;

			}		
		}
		
		BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + sampleFileName));
		
		CSVReader reader = new CSVReader(buffer, '\t', 'π');
		
		String[] headers;
	
		while((headers = reader.readNext()) != null) {

			boolean isComment = ( headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() ) ? true: headers[0].isEmpty() ? true: false;
			
			if(isComment) {
				
				continue;
				
			} else {
				
				if(headers[patientIdCol].equalsIgnoreCase(managedInput.getPhsSubjectIdColumn())) continue;
				
				patientSet.add(headers[patientIdCol]);
				
			}
			
		}
		
		return patientSet;
	}
	public static String getVersion(BDCManagedInput managedInput) throws IOException {
		String subjMultiFile = BDCJob.getStudySubjectMultiFile(managedInput);
		
		if(subjMultiFile == null) {
			System.err.println("Missing subject multi file for " + managedInput.getStudyAbvName() + ":" + managedInput.getStudyIdentifier());
			return null;
		} else {
			for(String filePart: subjMultiFile.split("\\.")) {
				
				if(filePart.toLowerCase().startsWith("v")) {
					return filePart;
				}
					
			}
			return null;
		}
	}
	public static String getPhase(BDCManagedInput managedInput) throws IOException {
		String subjMultiFile = BDCJob.getStudySubjectMultiFile(managedInput);
		
		if(subjMultiFile == null) {
			System.err.println("Missing subject multi file for " + managedInput.getStudyAbvName() + ":" + managedInput.getStudyIdentifier());
			return null;
		} else {
			for(String filePart: subjMultiFile.split("\\.")) {
				
				if(filePart.matches("p[0-9]")) {
					return filePart;
				}
					
			}
			return null;
		}
	}

	public static Integer getVariableCountFromRawData(BDCManagedInput managedInput) throws IOException {
		String[] line;
		
		Map<String,Integer> phtCounts = new HashMap<>();
		
		File dataDir = new File(DATA_DIR);
		if(dataDir.isDirectory()) {
			final String phsOnly = managedInput.getStudyIdentifier();
			File[] files = dataDir.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.startsWith(phsOnly) && name.contains("pht") && name.endsWith(".txt")) {
						return true;
					}

					return false;
				}
			});
			for(File file: files) {
				CSVReader reader = BDCJob.readRawBDCDataset(file,false);
				String pht = BDCJob.getPht(file.getName());
				
				while((line = reader.readNext()) != null ) {
					if(line[0].startsWith("##")) {
						phtCounts.put(pht, line.length -1);
						break;
					}
				}
				
			}
		} else {
			System.err.println(dataDir + " is not a directory.");
		}
		
		if(phtCounts.isEmpty()) {
			return -1;
		} else {
			int count = 0;
			for(Integer x:phtCounts.values()) {
				count += x;
			}
			return count;
		}
		
	}
	


	/**
	 * Psuedo factory that just cast Abstract ManagedInput into BDCManagedInput
	 * Can be an ID factory when we have more solidified business rules across multiple products
	 * Works logically as at this point we know input must be BDC specific
	 * @return
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	protected static List<BDCManagedInput> getManagedInputs() throws IOException {

		return (List<BDCManagedInput>)(List<?>) ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);
		
	}
	
	protected static Map<String, Map<String, String>> getPatientMappings() throws IOException {
		
		return Job.getPatientMappings(ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT));
		
	}
	
	public static String getPhs(String dbGapFileName) {
		String[] fileNameArr = dbGapFileName.split("\\.");
		for(String str: fileNameArr) {
			if(str.contains("phs")) return str;
		}
		return null;
	}
	public static String[] getHeaders(CSVReader reader) throws IOException {

		String[] line;
		
		while((line = reader.readNext()) != null) {
			if(line[0].toLowerCase().startsWith("dbgap")) {
				return line;
			}
		}
		return null;
	}

	public static String[] getHeaders(File data) throws IOException {

		CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(data.getAbsolutePath())), '\t');

		String[] line;
		
		while((line = reader.readNext()) != null) {
			if(line[0].toLowerCase().startsWith("dbgap")) {
				return line;
			}
		}
		return null;
	}
	public static String[] getDataHeaders(File file) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()), StandardCharsets.ISO_8859_1)) {
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				
				String[] record = line.split(new Character('\t').toString());
				
				if(record[0] == null) continue;
				if(record[0].startsWith("#")) continue;
				if(record[0].toLowerCase().contains("dbgap")) return record;
				
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
}

