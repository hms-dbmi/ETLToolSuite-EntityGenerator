package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.opencsv.CSVReader;

import etl.jobs.Job;

public class UDNUpdateVCFIndex extends Job {
	private static final String MANIFEST_FILE = DATA_DIR + "UDN_file-manifest_2020-01-22.tsv";
	private static final String HPDSID_FILE = DATA_DIR + "hpdsid_to_udnid.csv";

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
	/**
	 * main execution thread
	 * @throws IOException
	 */
	private static void execute() throws IOException {
		Map<String,String> sampidToUdnIdLookup = buildIdMapFromManifestFile();
		
		Map<String,String> udnIdToHpdsId = buildUdnIdToHpdsIds();
		
		buildVcfIndex(sampidToUdnIdLookup, udnIdToHpdsId);
	}
	/**
	 * Take the two lookup maps and join them then build vcfindex file
	 * @param sampidToUdnIdLookup
	 * @param udnIdToHpdsId
	 * @throws IOException 
	 */
	private static void buildVcfIndex(Map<String, String> sampidToUdnIdLookup, Map<String, String> udnIdToHpdsId) throws IOException {
		Map<String,String> sampleidToHpdsId = new TreeMap<>();
		
		for(Entry<String,String> entry: sampidToUdnIdLookup.entrySet()) {
			if(udnIdToHpdsId.containsKey(entry.getValue())) {
				
				String hpdsid = udnIdToHpdsId.get(entry.getValue());
				sampleidToHpdsId.put(entry.getKey(), hpdsid);
				
			} else {
				System.err.println(entry.toString() + " missing from pheno load");
			}
			
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "vcfIndex.tsv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING )) {
			
			String linetowrite = "filename\tchromosome\tannotated\tgzip\tsample_ids\tpatient_ids\tsample_relationship\trelated_sample_ids\n";
			
			writer.write(linetowrite);
			
			StringBuilder nextline = new StringBuilder();
			nextline.append("/opt/local/hpds/vcfInput/full_udn_exome_calls.transform_csq.vcf.gz\tALL\t1\t1\t");
			
			for(String sampleid: sampleidToHpdsId.keySet()) {
				nextline.append(sampleid);
				nextline.append(",");
			}
			nextline.setLength(nextline.length() - 1);
			
			nextline.append('\t');
			
			for(String hpdsid: sampleidToHpdsId.values()) {
				nextline.append(hpdsid);
				nextline.append(",");
			}
			
			nextline.setLength(nextline.length() - 1);
			
			writer.write(nextline.toString());
		}
	}
	/**
	 * simple lookup tree map for hpds id via udn id
	 * @return
	 * @throws IOException 
	 */
	private static Map<String, String> buildUdnIdToHpdsIds() throws IOException {
		Map<String, String> map = new TreeMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(HPDSID_FILE))) {
			
			buffer.readLine();
			
			CSVReader reader = new CSVReader(buffer);
			String[] line;
			
			while((line = reader.readNext()) != null) {
				
				if(line.length <= 0 ) continue;

				map.put(line[1], line[0]);
				
				
			}
		}
		return map;
	}
	/**
	 * Simple lookup treemap to find udnids via sample id
	 * @return
	 * @throws IOException
	 */
	private static Map<String, String> buildIdMapFromManifestFile() throws IOException {
		Map<String, String> map = new TreeMap<>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(MANIFEST_FILE))) {
			CSVReader reader = new CSVReader(buffer, '\t');
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				
				if(line.length <= 0 ) continue;
				if(line[0].startsWith("#")) continue;
				if(line[0].equalsIgnoreCase("UDN_ID")) continue;
				map.put(line[1], line[0]);
				
				
			}
		}
		return map;
	}

}
