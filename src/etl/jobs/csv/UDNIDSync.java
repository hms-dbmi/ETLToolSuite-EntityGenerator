package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.jobs.Job;

public class UDNIDSync extends Job {

	private static final String MANIFEST_FILE = DATA_DIR + "UDN_file-manifest_2020-01-22.tsv";
	private static final String PM_FILE = DATA_DIR + "hpdsid_to_udnid.csv";
	private static final String SAMPLE_LIST = DATA_DIR + "vcfSampList.csv";
	private static final String PM_FILE_NEW = DATA_DIR + "PatientMappingNew.csv";
	private static final String PM_FILE_OLD = DATA_DIR + "PatientMappingOld.csv";

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
		// read manifest
		Map<String,String> udnid_sampid_map = new HashMap<String,String>();
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(MANIFEST_FILE))){

			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator('\t')
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();
	
			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
				
			List<String[]> records = csvreader.readAll();
			
			for(String[] line: records) {
				if(line[0] == null) continue;
				if(line[0].isEmpty()) continue;
				if(line[0].startsWith("#")) continue;
			
				udnid_sampid_map.put(line[1], line[0]);
			}
		// read patient mapping
		}
		Map<String,String> udnid_patid_map = new HashMap<String,String>();

		try(BufferedReader reader = Files.newBufferedReader(Paths.get(PM_FILE))){

			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(',')
					.withQuoteChar('"');
				
			RFC4180Parser parser = parserbuilder.build();
	
			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
				
			List<String[]> records = csvreader.readAll();
			
			for(String[] line: records) {
				if(line[0].equals("2998")) {
					System.out.println(line[1] + ":" + line[0]);
				}
				if(line[0] == null) continue;
				if(line[0].isEmpty()) continue;
				if(line[0].startsWith("#")) continue;
			
				udnid_patid_map.put(line[1], line[0]);
			}
		// read patient mapping
		}
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(SAMPLE_LIST))){

			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(',')
					.withQuoteChar('`');
				
			RFC4180Parser parser = parserbuilder.build();
	
			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
			
			String[] line;
			StringBuilder sampids = new StringBuilder();
			StringBuilder patids = new StringBuilder();
			
			List<String> UDN_IDS_MISSING_FROM_PHENO_DATA = new ArrayList<String>();
			
			List<String> VARIANTS_MISSING_FROM_MANIFEST_DOCUMENT = new ArrayList<String>();
			int x = 0;
			while((line = csvreader.readNext())!= null) {
				
				if(udnid_sampid_map.containsKey(line[0])) {
					
					String udnId = udnid_sampid_map.get(line[0]);
					
					if(udnid_patid_map.containsKey(udnId)) {
						
						String patNum = udnid_patid_map.get(udnId);
						System.err.println(patNum);
						if(patNum.equals("2998")) {
							System.out.println(udnId + ":" + line[0] + patNum);
						}						
						if(sampids.toString().isEmpty()) {
							
							sampids.append(line[0]);
							patids.append(patNum);
							
						} else {
							
							sampids.append(",");
							sampids.append(line[0]);
							
							patids.append(",");
							patids.append(patNum);
							
						}
						x++;
					} else {
						
						UDN_IDS_MISSING_FROM_PHENO_DATA.add(udnId + '\n');
						System.err.println("udnid does not exist in pheno data " + udnId);
						
					}					
					
				} else {
					
					VARIANTS_MISSING_FROM_MANIFEST_DOCUMENT.add(line[0] + '\n');
					System.err.println("vcf variant not found in manifest " + line[0]);
					
				}
				
			}
			// output to vcfIndex.tsv
			String lineToWrite = "/opt/local/hpds/vcfInput/full_udn_exome_calls.transform_csq.vcf.gz\tALL\t1\t1\t" + sampids.toString() + '\t' + patids.toString();
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "vcfIndex.tsv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)){
				writer.write(lineToWrite);
				writer.flush();
			}
			// output info files
			// variants missing from the vcf file.
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Variants_Missing_From_Manifest.csv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)){
				for(String var: VARIANTS_MISSING_FROM_MANIFEST_DOCUMENT) {
					writer.write(var);
					writer.flush();
				}
			}
			// UDN IDs missing from pheno data
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "UDN_IDs_Missing_From_Pheno_Data.csv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)){
				for(String var: UDN_IDS_MISSING_FROM_PHENO_DATA) {
					writer.write(var);
					writer.flush();
				}
			}
			System.out.println("Total patients mapped: " + x);
			mapOldMapping();
		}
		
	}


	private static void mapOldMapping() throws IOException {
		
		Map<String,String> oldPM = new HashMap<String,String>();
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(PM_FILE_OLD))){
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(',')
					.withQuoteChar('`');
				
			RFC4180Parser parser = parserbuilder.build();
	
			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
				
			List<String[]> records = csvreader.readAll();
			
			for(String[] rec: records) {
				
				oldPM.put(rec[1],rec[2]);
				
			}
			
		}
		
		Map<String,String> newPM = new HashMap<String,String>();
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(PM_FILE_NEW))){
			
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(',')
					.withQuoteChar('"');
				
			RFC4180Parser parser = parserbuilder.build();
	
			CSVReaderBuilder builder = new CSVReaderBuilder(reader)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
				
			List<String[]> records = csvreader.readAll();
			
			for(String[] rec: records) {
				
				newPM.put(rec[1],rec[2]);
				
			}
			
		}
		
		List<String> stringsToWrite = new ArrayList<String>();
		
		stringsToWrite.add("bad_patient_id" + ',' + "good_patient_id\n");
		
		for(Entry<String, String> oldIter: oldPM.entrySet()) {
			
			String udnId = oldIter.getKey();
			
			String oldId = oldIter.getValue();
			
			if(newPM.containsKey(udnId)) {
				
				stringsToWrite.add(oldId + ',' + newPM.get(udnId) + '\n');
				
			} else {
				System.err.println(udnId + " UDN_ID IS MISSING!");
			}
			
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Patient_ID_Mapping_Fix.csv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)){
			for(String str: stringsToWrite) {
				writer.write(str);
				writer.flush();
			}
		}
		
	}

}
