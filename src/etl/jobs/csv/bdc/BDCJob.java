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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.Job;

public abstract class BDCJob extends Job {
	public static Set<String> NON_DBGAP_STUDY = new HashSet<String>(){{
		
		add("ORCHID");
		add("HCT_FOR_SCD");
		add("RED_CORAL");
		
	}};
	private static List<String> CONSENT_HEADERS = new ArrayList<String>();


	static {

		CONSENT_HEADERS.add("consent".toLowerCase());
		CONSENT_HEADERS.add("consent_1217".toLowerCase());
		CONSENT_HEADERS.add("consentcol");
		CONSENT_HEADERS.add("gencons");

	}
	
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

	public static String findSubjectIdColumnName(Path subjectMultiFileName) throws IOException, NullPointerException {
		return findRawDataColumnNameAtIndex(subjectMultiFileName, 1);
	}

	public static String findRawDataColumnNameAtIndex(Path fileName, int columnIndex) throws IOException, NullPointerException {
		if(fileName == null) return null;
		BufferedReader buffer = Files.newBufferedReader(fileName);
		try (CSVReader reader = new CSVReader(buffer, '\t', 'π')) {
			String[] headers;
			String columnName = "";
			int row = 0;

			while((headers = reader.readNext()) != null) {
				boolean isComment = headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() || headers[0].isEmpty();
				
				if(isComment) {
					row++;
					continue;
					
				} else {
					
					columnName = headers[columnIndex];
					System.out.println("Column name is: " + columnName + ". Found in file row " + row);
					return columnName;
				}
				
			}
			
		}
		return null;
	}

	public static int findRawDataColumnIdx(Path fileName, String columnName) throws IOException {
		if(fileName == null) return -1;
		BufferedReader buffer = Files.newBufferedReader(fileName);
		try (CSVReader reader = new CSVReader(buffer, '\t', 'π')) {
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
		}
		
		return -1;  // not found
			
	}
	
	public static File FindDictionaryFile(String fileName) {
		if(!fileName.startsWith("phs")) {
			System.err.println("Bad Filename - Expecting a valid accessions phsxxxxxx for: " + fileName);
		
		}
		
		File dataDir = new File(DATA_DIR);
		
		String phs = null;
		
		
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
	public static File FindDictionaryFile(String fileName, String dataDirectory) {
		if(!fileName.startsWith("phs")) {
			System.err.println("Bad Filename - Expecting a valid accessions phsxxxxxx for: " + fileName);
		
		}
		
		File dataDir = new File(dataDirectory);
		
		String phs = null;
		
		
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
	
	public static String getPht(String dbGapFileName) {
		String[] fileNameArr = dbGapFileName.split("\\.");
		for(String str: fileNameArr) {
			if(str.contains("pht")) return str;
			if (str.contains("subjects")) return str;
			if (str.contains("samples")) return str;
		}
		return null;
	}
	
	public static String getPht(String[] dbGapFileNameArr) {
		for(String str: dbGapFileNameArr) {
			if(str.contains("pht")) return str;
			if (str.contains("subjects")) return str;
			if (str.contains("samples")) return str;
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
		
		File dataDir = new File(DATA_DIR + "decoded/");
		
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
	
			if(fileNames.length == 1) {
				return fileNames[0];
			}
			else return null;
		} else {
			throw new IOException(dataDir + " is not a directory.  Check datadir parameter", new Throwable().fillInStackTrace());
		}
	}
	
	public static String getStudySubjectMultiFile(BDCManagedInput managedInput, String directory) throws IOException {
		File dataDir = new File(directory);
		
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
	
	public static String[] getStudySampleMultiFile() throws IOException {
		File dataDir = new File(DATA_DIR);
						
		if(dataDir.isDirectory()) {
			
			String[] fileNames = dataDir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					if(name.toLowerCase().contains("sample.multi") && name.toLowerCase().endsWith(".txt")) {
						return true;
					} else {
						return false;
					}
				}
			});
	
			
			return fileNames;
		} else {
			throw new IOException(dataDir + " is not a directory.  Check datadir parameter", new Throwable().fillInStackTrace());
		}		
	}
	public static String getStudySampleMultiFile(BDCManagedInput managedInput) throws IOException {
		File dataDir = new File(DATA_DIR + "decoded/");
				
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
	    		
	    		
	    		List<Node> valueNodes = new ArrayList<Node>();
	    		
	    		for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
	
	        		Node node2 = variableChildren.item(idx2);
	        			        		
	        		if(node2.getNodeName().equalsIgnoreCase("name")) name = node2.getTextContent();
	        		
	        		
	        		if(node2.getNodeName().equalsIgnoreCase("value")) valueNodes.add(node2);
	    		}
	    		
	    		Map<String,String> valueMap = new HashMap<String,String>(); 
	    		
	    		//if(!type.equalsIgnoreCase("encoded")) continue;
	    		
	    		for(Node vnode: valueNodes) {
	    			
	    			
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
	public static int getConsentIdx(String fileName) throws IOException {

		try (BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + fileName))) {
			try (CSVReader reader = new CSVReader(buffer, '\t', 'π')) {
				String[] headers;

				while ((headers = reader.readNext()) != null) {

					boolean isComment = (headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty())
							? true
							: headers[0].isEmpty() ? true : false;

					if (isComment) {

						continue;

					} else {

						break;

					}

				}

				int consentidx = -1;
				int x = 0;
				System.out.println("Checking for header matching expected consent name block");
				for (String header : headers) {
					System.out.println("Checking " + header);
					if (CONSENT_HEADERS.contains(header.toLowerCase())) {
						System.out.println("Consent header found = " + header);
						consentidx = x;
						break;
					}
					x++;
				}
				x = 0;
				if (consentidx == -1) {

					System.out.println("Consent header not found in expected header block.  Searching dynamically for any header containing 'consent'");
					for (String header : headers) {

						if (header.toLowerCase().contains("consent")) {
							System.out.println("Consent header found = " + header);
							consentidx = x;
							break;
						}
						x++;
					}

				}

				return consentidx;
			}
		}
	}

	public static List<String> getPatientSetForConsentFromRawData(String subjectFileName, BDCManagedInput managedInput, List<String> consentHeaders, String consent_group_code) throws IOException {
		List<String> patientSet = new ArrayList<>();
		if(!new File(DATA_DIR + "raw/" + subjectFileName).exists()) return patientSet;
		int cgcidx = getConsentIdx( "raw/" + subjectFileName);

		int patientIdCol = 1;
		
		
		if(cgcidx != -1) {
			
			BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR +"raw/" + subjectFileName));
			
			try (CSVReader reader = new CSVReader(buffer, '\t', 'π')) {
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
			
		}
		
		return patientSet;
	}
	
	public static List<String> getPatientSetFromSampleFile(String subjectFileName, String sampleFileName, BDCManagedInput managedInput) throws IOException {
		List<String> patientSet = new ArrayList<>();
		try{
		String subjectColumnName = findSubjectIdColumnName(Paths.get(DATA_DIR + "raw/" + subjectFileName));
		int patientIdCol = findRawDataColumnIdx(Paths.get(DATA_DIR + "raw/" + sampleFileName), subjectColumnName); 
		
		if(patientIdCol == -1) {
			System.err.println("Error no subject id column found for " + managedInput.getStudyIdentifier() + " - " +
					subjectColumnName + " in sample multi file for " + sampleFileName
						);
			
			patientIdCol = findRawDataColumnIdx(Paths.get(DATA_DIR + "raw/" + sampleFileName), "SUBJECT_ID");
					
		}
		
		
		BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "raw/" + sampleFileName));
		
		try (CSVReader reader = new CSVReader(buffer, '\t', 'π')) {
			String[] headers;

			while((headers = reader.readNext()) != null) {

				boolean isComment = ( headers[0].toString().startsWith("#") || headers[0].toString().trim().isEmpty() ) ? true: headers[0].isEmpty() ? true: false;
				
				if(isComment) {
					
					continue;
					
				} else {
					
					if(headers[patientIdCol].equalsIgnoreCase(subjectColumnName)) continue;
					
					patientSet.add(headers[patientIdCol]);
					
				}
				
			}
		}
		
		return patientSet;
	}
		catch(IOException e){
			System.err.println("Subject and/or sample multi not found for " + managedInput.getStudyIdentifier() + " at " + DATA_DIR + "raw/ - Please check your directories and try again.");
			System.exit(255);
		}
		return null;

	}
	public static String getVersion(BDCManagedInput managedInput) throws IOException {
		String subjMultiFile = BDCJob.getStudySubjectMultiFile(managedInput);
		
		if(subjMultiFile == null) {
			System.err.println("Missing subject multi file for " + managedInput.getStudyAbvName() + ":" + managedInput.getStudyIdentifier());
			return null;
		} else {
			for(String filePart: subjMultiFile.split("\\.")) {
				
				if(filePart.matches("v[0-9]+")) {
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
				if(filePart.matches("p[0-9]+")) {
					return filePart;
				}
				
			}
			return null;
		}
	}

	public static Integer getVariableCountFromRawData(BDCManagedInput managedInput) throws IOException {
		String[] line;
		
		Map<String,Integer> phtCounts = new HashMap<>();
		
		File dataDir = new File(DATA_DIR + "raw/");
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

		try (CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(data.getAbsolutePath())), '\t')) {
			String[] line;
			
			while((line = reader.readNext()) != null) {
				if(line[0].toLowerCase().startsWith("dbgap")) {
					return line;
				}
			}
		}

		return null;
	}
	public static String[] getDataHeaders(File file) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(file.getAbsolutePath()), StandardCharsets.ISO_8859_1)) {
			System.out.println(file.getAbsolutePath());
			String line;
			
			while((line = buffer.readLine()) != null) {
				System.out.println(line);
				String[] record = line.split("\t");
				try{
					if(record[0] == null) continue;
				}
				catch(ArrayIndexOutOfBoundsException e){
					System.out.println("caught array exception");
					continue;
				}
				if(record[0].startsWith("#")) continue;
				if(record[0].trim().isEmpty()) continue;
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
	
	public static boolean hasPatientMapping(BDCManagedInput managedInput) throws IOException {
		File file = new File(DATA_DIR);

		if(file.isDirectory()) {
			
			String[] files = file.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					
					if(name.equalsIgnoreCase(managedInput.getStudyAbvName() + "_PatientMapping.v2.csv")) {
						return true;
					}
					
					return false;
					
				}
				
			});
			
			if(files.length > 0) {
				return true;
			} else {
				return false;
			}
			
		} else {
			throw new IOException(DATA_DIR + " is not a directory!");
		}
	}
	
	public static Map<String,String> getDbgapToSubjectIdMappingFromRawData(BDCManagedInput managedInput) throws IOException {
		Map<String,String> subjIds = new HashMap<>();
		
		String multi = BDCJob.getStudySubjectMultiFile(managedInput);
		
		CSVReader reader = BDCJob.readRawBDCDataset(new File(DATA_DIR + multi), true);		
		
		int idx = 1;
		
		String[] line;
		
		while((line = reader.readNext())!= null) {
			
			subjIds.put(line[0],line[idx]);
		}
		
		return subjIds;
	}
	
	public static List<String> getDbgapSubjIdsFromRawData(BDCManagedInput managedInput) throws IOException {
		List<String> subjIds = new ArrayList<String>();
		
		String multi = BDCJob.getStudySubjectMultiFile(managedInput);
		
		CSVReader reader = BDCJob.readRawBDCDataset(new File(DATA_DIR + multi), true);		
		
		String[] line;
		
		while((line = reader.readNext())!= null) {
			subjIds.add(line[0]);
		}
		
		return subjIds;
	}
	
	public static List<String> getDbgapSubjIdsFromRawData(BDCManagedInput managedInput, String dataDir) throws IOException {
		List<String> subjIds = new ArrayList<String>();
		
		String multi = BDCJob.getStudySubjectMultiFile(managedInput, dataDir);
		
		CSVReader reader = BDCJob.readRawBDCDataset(new File(DATA_DIR + multi), true);		
		
		String[] line;
		
		while((line = reader.readNext())!= null) {
			subjIds.add(line[0]);
		}
		
		return subjIds;
	}

	public static Map<String, String> getDbgapToSubjectIdMappingFromRawData(BDCManagedInput managedInput,
			Map<String, String> overrides) throws IOException {

		Map<String,String> subjIds = new HashMap<>();
		
		String multi = BDCJob.getStudySubjectMultiFile(managedInput);
		
		CSVReader reader = BDCJob.readRawBDCDataset(new File(DATA_DIR + multi), true);		
		
		int idx = -1;
		
		if(overrides.containsKey(managedInput.getStudyAbvName())) {
			idx = BDCJob.findRawDataColumnIdx(Paths.get(DATA_DIR + multi), overrides.get(managedInput.getStudyAbvName()));
		} else {
			idx = 1;
		}
		String[] line;
		
		while((line = reader.readNext())!= null) {
			
			subjIds.put(line[0],line[idx]);
		}
		
		return subjIds;
	}


	
	
}

