package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;

import etl.jobs.mappings.Mapping;
import etl.jobs.mappings.PatientMapping;

public class ORCHIDDataPrep extends BDCJob {

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
			e.printStackTrace();
			System.err.println(e);
			
		}		

	}
	public class DataDictionary {
		public String name;
		public String label;
		public String type;
		public String format;
		public String informat;
		public String varnum;
		
		public DataDictionary(String[] line) {
			this.name = line[0];
			this.label = line[1];
			this.type = line[2];
			this.format = line[3];
			this.informat = line[4];
			this.varnum = line[5];
		}
		
	}
	private static void execute() throws IOException {
		Map<String, DataDictionary> dataDict = readDataDictionary(Paths.get(DATA_DIR + "alldata_dd.csv"));
		dataDict.putAll(readDataDictionary(Paths.get(DATA_DIR + "derived_vars_dd.csv")));
		
		//Set<Mapping> set = buildMappings(dataDict,Paths.get(DATA_DIR + ("alldata.csv")));
		
		//set.addAll(buildMappings(dataDict,Paths.get(DATA_DIR + ("derived_vars.csv"))));
		
		Set<Mapping> set2 = buildMappings(dataDict,Paths.get(DATA_DIR + ("ORCHID_manuscript_dataset_idenfied.csv")));
		
		writeMapping(set2);
		
		updatePatientMapping();
	}

	private static void updatePatientMapping() throws IOException {
		
			
		List<PatientMapping>  patientMappings = PatientMapping.readPatientMappingFile("data/ORCHID_PatientMapping.v2_OLD.csv");
		
		List<String[]> orchidPatientIds = getPatientIdMap();
		
		
		for(PatientMapping pm: patientMappings) {
			for(String[] arr: orchidPatientIds) {
				if(pm.getSourceId().equalsIgnoreCase(arr[arr.length-1])) {
					System.out.println(arr[arr.length-1]);
					pm.setSourceId(arr[1]);
				}
			}
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "ORCHID_PatientMapping.v2.csv"),
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(PatientMapping pm: patientMappings) {
				
				writer.write(pm.toCSV());
				
			}
		}
	}

	private static List<String[]> getPatientIdMap() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "orchid_id_key.csv"))) {
			CSVReader reader = new CSVReader(buffer);
			
			return reader.readAll();
		}
		
	}

	private static void writeMapping(Set<Mapping> mappings) throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mapping.csv"))) {
			
			writer.write(Mapping.printHeader());
			
			for(Mapping mapping: mappings) {
				writer.write(mapping.toCSV() + '\n');
			}
			
		}
		
	}

	private static Set<Mapping> buildMappings(Map<String, DataDictionary> dataDicts, Path path) throws IOException {
		Set<Mapping> mappings = new TreeSet<Mapping>(new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				return o1.getRootNode().compareTo(o2.getRootNode());
			}
		});
		try(BufferedReader buffer = Files.newBufferedReader(path,StandardCharsets.ISO_8859_1)) {
			
			String line;
			
			/*while((line = buffer.readLine())!=null) {
				System.out.println(line);
			}*/
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] headersArr = reader.readNext();
			
			List<String> headers = Arrays.asList(headersArr);
			
			for(String header: headers) {
				
				if(!dataDicts.containsKey(header)) continue;
				
				DataDictionary dataDict = dataDicts.get(header);
				
				Mapping mapping = new Mapping();
				
				int colIndex = headers.indexOf(header);
				
				String fileName = path.getFileName().toString();
				
				mapping.setKey(fileName + ':' + colIndex);
				
				String label = dataDict.label;
				
				/*if(label.isEmpty()) {
					
					System.err.println("Data Dict missing label for: " + header + " in data set " + fileName);
					
					continue;
				}*/
				
				
				//label.replaceAll("[^\\x00-\\x7F]", "");
				String conceptPath = 'µ' + "COVID19-ORCHID ( phs002299 )" + 'µ' + header + 'µ';
				
				mapping.setRootNode(conceptPath);
				
				mapping.setDataType(dataDict.type.equalsIgnoreCase("num") ? "NUMERIC": "TEXT");
				
				mappings.add(mapping);
				
			}
		}
		
		return mappings;
	}

	private static Map<String,DataDictionary> readDataDictionary(Path dataDict) throws IOException {
		
		Map<String,DataDictionary> dds = new HashMap<>();
		try(BufferedReader buffer = Files.newBufferedReader(dataDict)) {
						
			CSVReader reader = new CSVReader(buffer);
			
			String[] line = reader.readNext();
			
			while((line = reader.readNext()) != null) {
				DataDictionary dd = new ORCHIDDataPrep().new DataDictionary(line);
				dds.put(line[0], dd);
			}
		}
		return dds;
	}

	private static void setClassVariables(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
