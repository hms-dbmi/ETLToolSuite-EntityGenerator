package etl.drivers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.exceptions.RequiredFieldException;
import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.job.entity.i2b2tm.PatientDimension;
import etl.job.entity.patientgen.MappingHelper;
import etl.utils.Utils;

public class FactGenerator {	
	private static boolean SKIP_HEADERS = true;

	private static String WRITE_DIR = "./completed/";
	
	private static String MAPPING_FILE = "./mappings/mapping.csv";

	private static boolean MAPPING_SKIP_HEADER = true;

	private static char MAPPING_DELIMITER = ',';

	private static char MAPPING_QUOTED_STRING = '"';

	private static final char DATA_SEPARATOR = ',';

	private static final char DATA_QUOTED_STRING = '"';

	private static Integer START_DATE_COL = null;

	private static Integer END_DATE_COL = null;
	
	private static Integer PATIENT_COL = 0;

	private static String DATA_DIR = "./data/";

	private static String TRIAL_ID = "DEFAULT";
		
	private static Integer ENCOUNTER_COL = -1;
	
	private static Integer INSTANCE_COL = -1;
	
	// hash map with patient dim attribute keyword and filename and column as value

	public static void main(String[] args) throws Exception {
		setVariables(args);
		
		execute();
	}
	
	private static void execute() throws RequiredFieldException {
		List<Mapping> mappings = new ArrayList<Mapping>();

		try {
			mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		} catch (IOException e) {
			System.err.println("Error generating mapping file!");
			System.err.println(e.getMessage());
		} 
		
		try {
			doFactGenerator(mappings);
			doPatientSecurityGenerator();
		} catch (IOException e) {
			System.err.println("Error generating facts!");
			System.err.println(e.getMessage());		
		}
	}
	private static void doPatientSecurityGenerator() throws IOException {
		String fileName = "PatientDimension.csv";
		Set<ObservationFact> facts = new HashSet<ObservationFact>();
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + fileName))){
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();

			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();		
			
			List<String[]> recs = csvreader.readAll();
						
			for(String[] rec: recs) {
				String patientNum = rec[0];
				if(!patientNum.isEmpty()) {
					ObservationFact obs = new ObservationFact();
					obs.setPatientNum(patientNum);
					obs.setConceptCd("SECURITY");
					obs.setEncounterNum("-1");
					obs.setInstanceNum("-1");
					obs.setModifierCd(TRIAL_ID);
					obs.setTvalChar("EXP:PUBLIC");
					
					obs.setValueFlagCd("@");
					obs.setQuantityNum("1");
					obs.setLocationCd("@");
					obs.setSourceSystemCd(TRIAL_ID);
					
					facts.add(obs);
				}
					
			}
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"), StandardOpenOption.APPEND)){

			Utils.writeToCsv(buffer, facts.stream().collect(Collectors.toList()), DATA_QUOTED_STRING, DATA_SEPARATOR);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error writing Observation Facts");
			System.err.println(e);
		} catch (CsvDataTypeMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvRequiredFieldEmptyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	
	private static void doFactGenerator(List<Mapping> mappings) throws IOException {
		
		String defaultDate = new Date().toString();
		BufferedWriter delete = new BufferedWriter(new FileWriter(WRITE_DIR + File.separatorChar + "ObservationFact.csv"));
		delete.close();
		Set<ObservationFact> facts = new HashSet<ObservationFact>();

		mappings.forEach(mapping -> {
			String[] options = mapping.getOptions().split(":");
			for(String option: options) {
				if(option.split("=").length != 2) continue;
				String optionkey = option.split("=")[0];
				optionkey = optionkey.replace(String.valueOf(MAPPING_QUOTED_STRING), "");
				if(optionkey.equalsIgnoreCase("patientcol")) {
					PATIENT_COL = Integer.valueOf(option.split("=")[1]);
				} else if(optionkey.equalsIgnoreCase("startdatecol")) {
					START_DATE_COL = Integer.valueOf(option.split("=")[1]);
				} else if(optionkey.equalsIgnoreCase("enddatecol")) {
					END_DATE_COL = Integer.valueOf(option.split("=")[1]);
				}
			}
			String fileName = mapping.getKey().split(":")[0];
			Integer column = new Integer(mapping.getKey().split(":")[1]);
			if(!Files.exists(Paths.get(DATA_DIR + File.separatorChar + fileName))) {
				System.err.println(DATA_DIR + File.separatorChar + fileName + " does not exist.");
				return;
			}
			try(BufferedReader reader = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + fileName))){
				RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
						.withSeparator(DATA_SEPARATOR)
						.withQuoteChar(DATA_QUOTED_STRING);
					
				RFC4180Parser parser = parserbuilder.build();

				CSVReaderBuilder builder = new CSVReaderBuilder(reader)
						.withCSVParser(parser);
				
				CSVReader csvreader = builder.build();
				
				if(SKIP_HEADERS) csvreader.readNext();

				List<String[]> records = csvreader.readAll();
								
				records.forEach(record ->{

					ObservationFact obs = new ObservationFact();
					
					String conceptCd = mapping.getDataType().equalsIgnoreCase("numeric") ?
							mapping.getRootNode() :mapping.getRootNode() + record[column] + "\\"; 

					String encounterNum = ENCOUNTER_COL == -1 ? "-1" : record[ENCOUNTER_COL];
					String startDate = START_DATE_COL == null ? defaultDate : record[START_DATE_COL];
					String endDate = END_DATE_COL == null ? defaultDate : record[END_DATE_COL];

					String instanceNum = INSTANCE_COL == -1 ? "-1" : record[INSTANCE_COL];
					String tvalChar = mapping.getDataType().equalsIgnoreCase("TEXT") ? record[column]: "E";
					String nvalNum = mapping.getDataType().equalsIgnoreCase("TEXT") ? "": record[column];
					String patientNum = record[PATIENT_COL].isEmpty() ? null: record[PATIENT_COL];
												
					if(patientNum == null) return;
					
					obs.setPatientNum(patientNum);
					obs.setConceptCd(conceptCd);
					obs.setEncounterNum(encounterNum);
					obs.setInstanceNum(instanceNum);
					obs.setModifierCd("@");
					obs.setTvalChar(tvalChar);
					obs.setNvalNum(nvalNum);
					obs.setSourceSystemCd(TRIAL_ID);
					obs.setStartDate(startDate);
					obs.setEndDate(endDate);
					
					facts.add(obs);

				});

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println(e.getMessage());
			}			
		});	
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"), StandardOpenOption.CREATE, StandardOpenOption.CREATE)){
			try {
				Utils.writeToCsv(buffer, facts.stream().collect(Collectors.toList()), DATA_QUOTED_STRING, DATA_SEPARATOR);
			} catch (CsvDataTypeMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CsvRequiredFieldEmptyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error writing Observation Facts");
			System.err.println(e);
		} 
	}

	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase( "-patientcol" )){
				PATIENT_COL = new Integer(checkPassedArgs(arg, args));
			} 
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
