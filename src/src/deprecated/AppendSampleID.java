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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import etl.jobs.Job;

/**
 * This method is meant to append mapping files with 
 * NWD values from sample.multi file
 * Only holding on to it if needed right now it is useless
 * All this logic should go to a BDCPatientMapping object if it is needed.
 * @author Tom
 *
 */
public class AppendSampleID extends Job {

	/**
	 * 
	 */
	private static final long serialVersionUID = -834479314465676588L;

	private static String STUDY_ID_W_ACCESSIONS = "studyid_with_accessions.csv";

	private static List<String> sampleIdsFoundInVcfIndex = new ArrayList<>();
	
	private static List<String> sampleIdsNotFoundInVcfIndex = new ArrayList<>();

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
				
		String topmedAccession = getTopmedAccession();

		String sampleFile = getSampleFile(topmedAccession);
		
		if(sampleFile == null) throw new IOException("missing sample file in " + DATA_DIR); 
		// add NWD value to patient mappings
		addSampleIdstoPatientMapping(sampleFile);
		
	}

	private static String getTopmedAccession() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + STUDY_ID_W_ACCESSIONS))){
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				System.out.println(line);
				String[] arr = line.split(",");
				System.out.println(arr.length);
				if(arr[0].isEmpty()) continue;
				if(arr[2].isEmpty()) continue;
				if(TRIAL_ID.equalsIgnoreCase(arr[0])) {
					System.out.println(Arrays.asList(arr[0]));
					return arr[2];
					
				}
	
			}
			
		}
		return "";
	}

	private static String getSampleFile(String topmedAccession) {
		File dataDir = new File(DATA_DIR);
		
		if(dataDir.isDirectory()) {
			
			for(String f: dataDir.list()) {
				// if this is not the topmed accession for this study continue
				if(!f.toLowerCase().contains(topmedAccession.toLowerCase())) continue;
					
				if(!f.toLowerCase().contains("sample.multi")) continue;
				
				if(!f.toLowerCase().contains("txt")) continue;
				
				return f;
			}
		}
		return null;
	}

	private static void addSampleIdstoPatientMapping(String sampleFile) throws IOException {
		// read patient mapping
		Map<String,Map<String,String>> patientMappingWSampleId = new HashMap<>();
		
		setNewPatientMapping(patientMappingWSampleId);
		
		if(patientMappingWSampleId.isEmpty()) throw new IOException("Missing Patient Mappings check config file! - " +  PATIENT_MAPPING_FILE);
		
		addSampleIds(sampleFile, patientMappingWSampleId);
		
		Map<String,String> vcfMap = getVcfMap();
		
		syncPatients(vcfMap,patientMappingWSampleId);
		
		writeNewPatientMappings(patientMappingWSampleId);
	}

	private static void setNewPatientMapping(Map<String, Map<String, String>> patientMappingWSampleId) throws IOException {
	
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(PATIENT_MAPPING_FILE))){
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				if(line[0].isEmpty()) continue;
				if(line.length != 3)  continue;
				if(!NumberUtils.isCreatable(line[2])) continue;
				
				// append new element to hold sample id
				Map<String,String> innerMap = new HashMap<>();
				
				innerMap.put("hpds_id", line[2].trim());
				innerMap.put("sample_id", "");
				
				// map that stores the new patient mapping with sample id
				// key = hpds_patient_num 
				// value = another map that stores keys ( dbgapsubjid, sampleid ) and their values
				
				patientMappingWSampleId.put(line[0], innerMap);
	
			}
			
		}	
		
	}

	private static void addSampleIds(String sampleFile, Map<String, Map<String, String>> patientMappingWSampleId) throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + sampleFile))) {
			RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
					.withSeparator(DATA_SEPARATOR)
					.withQuoteChar(DATA_QUOTED_STRING);
				
			RFC4180Parser parser = parserbuilder.build();
	
			CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
					.withCSVParser(parser);
			
			CSVReader csvreader = builder.build();
			
			int sampleIdColIdx = 0;
			
			boolean foundSampleId = false;
			String[] line;
			
			while((line = csvreader.readNext()) != null) {
				// find once on first line of real data.
				if(!foundSampleId) {
					int idx = 0;
					for(String cell : line ) {
						if(cell.contains("NWD")) {
							sampleIdColIdx = idx;
						    foundSampleId = true;
							break;
						}
						idx++;
					}
					
				} else {
					
					if(line[0].isEmpty()) continue;
					
					patientMappingWSampleId.get(line[0].trim()).put("sample_id", line[sampleIdColIdx].trim());
					
				}
			}
		}
		
		
	}

	private static Map<String, String> getVcfMap() throws IOException {
		Map<String,String> vcfMap = new HashMap<>();
	
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "vcf_sync.csv"))) {
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				String[] line2 = line.split("\t");
				if(line2 == null || line2.length != 2) continue;
				
				vcfMap.put(line2[0].trim(), line2[1].trim());
				
			}
			
		}
	
		return vcfMap;
	}

	private static void syncPatients(Map<String, String> vcfMap,
			Map<String, Map<String, String>> patientMappingWSampleId) throws IOException {
	
		for(Entry<String, Map<String, String>> entry: patientMappingWSampleId.entrySet()) {
			//	innerMap.put("hpds_id", line[2]);
			//  innerMap.put("sample_id", "");
			
			String sampleId = entry.getValue().get("sample_id");
			String oldHpdsId = entry.getValue().get("hpds_id");
			
			if(sampleId.isEmpty()) continue; // not sequenced
			
			if(!vcfMap.containsKey(sampleId.trim())) {
				System.out.println(sampleId + " not found in vcfIndex!");
				sampleIdsNotFoundInVcfIndex.add(sampleId);
				continue;
			}
			
			sampleIdsFoundInVcfIndex.add(sampleId);
			
			String newHpdsId = vcfMap.get(sampleId.trim());
			
			// find current record that has hpdsid
			for(Entry<String, Map<String, String>> entry2: patientMappingWSampleId.entrySet()) {
				if(entry2.getValue().get("hpds_id").trim().equals(newHpdsId)) {
					entry2.getValue().put("hpds_id", oldHpdsId);
				}
			}
			
			entry.getValue().put("hpds_id", newHpdsId);
		}
		/// print out new mapping file and what vcf ids were found / not found.
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_SampleIds_Not_Found_in_vcfIndex.txt"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			for(String ltw: sampleIdsNotFoundInVcfIndex) {
				writer.write(ltw + '\n');
			}
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_SampleIds_Found_in_vcfIndex.txt"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			for(String ltw: sampleIdsFoundInVcfIndex) {
				writer.write(ltw + '\n');
			}
		}
		
	}

	private static void writeNewPatientMappings(Map<String, Map<String, String>> patientMappingWSampleId) throws IOException {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_PatientMapping.v2.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
		
			//innerMap.put("hpds_id", line[2].trim());
			//innerMap.put("sample_id", "");
			writer.write(toCsv(new String[] {
					"SOURCE_ID", "STUDY_ID", "HPDS_PATIENT_NUM", "SAMPLE_ID", "IN_CURRENT_VCF_INDEX"
			}));
			
			for(Entry<String,Map<String,String>> entry :patientMappingWSampleId.entrySet()) {
				Boolean isInVcfIndex = false;
				if(sampleIdsFoundInVcfIndex.contains(entry.getValue().get("sample_id"))) isInVcfIndex = true;
				writer.write(toCsv(new String[] {
						entry.getKey(), TRIAL_ID, entry.getValue().get("hpds_id"), entry.getValue().get("sample_id"), isInVcfIndex.toString()
				}));
				
			}
		}
		
	}

}
