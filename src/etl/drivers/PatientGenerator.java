package etl.drivers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.exceptions.RequiredFieldException;
import etl.job.entity.PatientMapping;
import etl.job.entity.i2b2tm.PatientDimension;
import etl.job.entity.i2b2tm.PatientTrial;
import etl.job.entity.patientgen.MappingHelper;
import etl.utils.Utils;

public class PatientGenerator {
	
	private static final List<LinkOption> options = null;
	
	private static final boolean SKIP_HEADERS = true;

	private static String WRITE_DIR = "./completed/";
	
	private static String MAPPING_FILE = "./mappings/mapping.csv";

	private static final boolean MAPPING_SKIP_HEADER = false;

	private static final char MAPPING_DELIMITER = ',';

	private static final char MAPPING_QUOTED_STRING = '"';

	private static final char DATA_SEPARATOR = ',';

	private static final char DATA_QUOTED_STRING = '"';

	private static String PATIENT_MAPPING_FILE = "./mappings/mapping.csv.patient";
	
	private static String DATA_DIR = "./data/";

	private static String TRIAL_ID = "DEFAULT";
	
	// hash map with patient dim attribute keyword and filename and column as value
	private static HashMap<String, MappingHelper> attributemap = new HashMap<String,MappingHelper>();
	static {
		attributemap.put("patientnum", new MappingHelper());
		attributemap.put("vitalstatusdcd", new MappingHelper());
		attributemap.put("birthdate", new MappingHelper());
		attributemap.put("sexcd", new MappingHelper());
		attributemap.put("ageinyearsnum", new MappingHelper());
		attributemap.put("languagecd", new MappingHelper());
		attributemap.put("racecd", new MappingHelper());
		attributemap.put("maritalstatuscd", new MappingHelper());
		attributemap.put("religioncd", new MappingHelper());
		attributemap.put("zipcd", new MappingHelper());
		attributemap.put("statecityzippath", new MappingHelper());
		attributemap.put("incomecd", new MappingHelper());
	}
	
	public static void main(String[] args) throws Exception {
		// Set application variables
		try {
			setVariables(args);
		// Execute patient generation
			execute();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	private static void execute() throws RequiredFieldException, IOException, InstantiationException, IllegalAccessException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
			// load patient mapping
			List<PatientMapping> patientMappings = !PATIENT_MAPPING_FILE.isEmpty() ? PatientMapping.class.newInstance().generateMappingList(PATIENT_MAPPING_FILE, MAPPING_DELIMITER): new ArrayList<PatientMapping>();
			
			List<Map<String, String>> patientNumFile = new ArrayList<Map<String, String>>();
			int	patientNumCol = 0;
					
			for(PatientMapping pm: patientMappings) {
				Set<String> attrKeys = attributemap.keySet();
				
				buildAttributes(pm,attrKeys);

			}
			
			if(attributemap.get("patientnum").getFileKeys().isEmpty()) {
				throw new RequiredFieldException("No patient ids found - check mapping file patientnum column is valid.");
			}
			
			Map<String, PatientDimension> pdCollection = new HashMap<String, PatientDimension>();
			
			for(String attrkey: attributemap.keySet()) {
				
				for(String mh: attributemap.get(attrkey).getFileKeys()) {
					
					String[] strarr = mh.split(":");

					if(strarr.length < 2) continue;
					
					String fileName = strarr[0];
					Integer colindex = new Integer(strarr[1]);
					// if patientcolumn doesnt exist expect patientnum to be first column;
					Integer patientIndex = strarr.length == 3 ? new Integer(strarr[2]): lookupPatientNumCol(fileName, attributemap);
					
					doPatientReader(attrkey, fileName,colindex,patientIndex,pdCollection);
					
				}
			}
			
		Set<PatientTrial> setPt = new HashSet<PatientTrial>();
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "PatientDimension.csv"))){
			
			pdCollection.entrySet();
			for(Entry<String, PatientDimension> entry: pdCollection.entrySet()) {
				PatientDimension pd = entry.getValue();
				if(!pd.getPatientNum().isEmpty()) {
					
					pd.setSourceSystemCD(TRIAL_ID + ":" + pd.getPatientNum());
					
					PatientTrial pt = new PatientTrial();
					pt.setPatientNum(pd.getPatientNum());
					pt.setTrial(TRIAL_ID);
					setPt.add(pt);

				}
			}
						
			List<PatientDimension> patients = pdCollection.values().stream().collect(Collectors.toList());
			Utils.writeToCsv(buffer, patients, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "PatientTrial.csv"))){
			
			Utils.writeToCsv(buffer, setPt.stream().collect(Collectors.toList()), DATA_QUOTED_STRING, DATA_SEPARATOR);

		} 
	}

	private static Integer lookupPatientNumCol(String fileName, HashMap<String, MappingHelper> attributemap) {
		if(attributemap.containsKey("patientnum")) {
			MappingHelper mh = attributemap.get("patientnum");
			for(String files: mh.getFileKeys()) {
				String _filename = files.split(":")[0];
				String patcol = files.split(":")[1];
				if(_filename.equalsIgnoreCase(fileName)) {
					return new Integer(patcol);
				}
			}
		}
		return null;
	}

	private static void doPatientReader(String attrkey, String fileName, Integer colindex,
			Integer patientIndex, Map<String, PatientDimension> pdCollection) {
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + fileName))){
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
			
			List<String[]> records = csvreader.readAll();
			
			records.parallelStream().forEach(record ->{
				String patientnum = record[patientIndex];
				
				Set<String> patientNums = pdCollection.keySet();
				
				if(patientNums.contains(patientnum)) {
					
					PatientDimension pd = pdCollection.get(patientnum);
					pd.setPatientNum(record[patientIndex]);
					setAttribute(pd,attrkey,record,colindex);
					pdCollection.put(record[patientIndex], pd);
					
				} else {
					
					PatientDimension pd = new PatientDimension();
					pd.setPatientNum(record[patientIndex]);
					setAttribute(pd,attrkey,record,colindex);
					pdCollection.put(record[patientIndex], pd);
					
				}
				
			});;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		}		
	}

	private static void setAttribute(PatientDimension pd, String attrkey, String[] record, Integer colindex) {

		if(attrkey.equalsIgnoreCase("patientnum")) pd.setPatientNum(record[colindex]);
		if(attrkey.equalsIgnoreCase("vitalstatusdcd")) pd.setVitalStatusCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("birthdate")) pd.setBirthDate(record[colindex]);
		if(attrkey.equalsIgnoreCase("sexcd")) pd.setSexCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("ageinyearsnum")) pd.setAgeInYearsNum(record[colindex]);
		if(attrkey.equalsIgnoreCase("languagecd")) pd.setLanguageCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("racecd")) pd.setRaceCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("maritalstatuscd")) pd.setMaritalStatusCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("religioncd")) pd.setReligionCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("zipcd")) pd.setZipCD(record[colindex]);
		if(attrkey.equalsIgnoreCase("statecityzippath")) pd.setStateCityZipPath(record[colindex]);
		if(attrkey.equalsIgnoreCase("incomecd")) pd.setIncomeCD(record[colindex]);
		
	}

	private static void buildAttributes(PatientMapping pm, Set<String> attrKeys) {
		
		MappingHelper mh = attributemap.get(pm.getPatientColumn().toLowerCase());
		
		String[] options = pm.getOptions().split(":");
		String patientcol = "";
		for(String option: options) {
			if(option.split("=").length != 2) continue;
			String optionkey = option.split("=")[0];
			if(optionkey.equalsIgnoreCase("patientcol")) {
				patientcol = option.split("=")[1];
			}
		}
		
		List<String> fileKeys = mh.getFileKeys();
		String fileKey = patientcol.isEmpty() ? pm.getPatientKey(): pm.getPatientKey() + ":" + patientcol;
		fileKeys.add(fileKey);
			
	}

	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase( "-patientmappingfile" )){
				PATIENT_MAPPING_FILE = checkPassedArgs(arg, args);
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
