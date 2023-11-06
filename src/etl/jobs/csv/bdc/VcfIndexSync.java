package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.opencsv.CSVReader;

import etl.job.entity.hpds.AllConcepts;

public class VcfIndexSync extends BDCJob {

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
		
		Map<String,String> vcfIndex = readVcfIndex();
		System.out.println("VCF INDEX SIZE: " + vcfIndex.size());
		Map<String, AllConcepts> acs = readGlobalConcepts();
		
		List<String[]> output = new ArrayList<>();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "vcfRemap.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

			for(Entry<String, String> rec: vcfIndex.entrySet()) {
				
				String vcfSampId = rec.getKey();
				
				String vcfPatNum = rec.getValue();
				
				String newPatNum = findVariablePatientNum(acs, vcfSampId);
				if(newPatNum.isEmpty()) continue;
				String[] str = new String[] { vcfSampId, vcfPatNum, newPatNum };
				writer.write(BDCJob.toCsv(str));
			 }
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mappings.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

			for(Entry<String, String> rec: vcfIndex.entrySet()) {
				
				String vcfSampId = rec.getKey();
				
				String vcfPatNum = rec.getValue();
				
				String newPatNum = findVariablePatientNum(acs, vcfSampId);
				if(newPatNum.isEmpty()) continue;
				String[] str = new String[] { vcfPatNum, newPatNum };
				writer.write(BDCJob.toCsv(str));
			 }
		}
	}

	private static String findVariablePatientNum(Map<String, AllConcepts> acs, String vcfSampId) {
		
		return acs.containsKey(vcfSampId) ? acs.get(vcfSampId).getPatientNum().toString() : "";
		
	}

	private static Map<String, AllConcepts> readGlobalConcepts() throws IOException {
		Map<String, AllConcepts> acs = new TreeMap<>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "GLOBAL_allConcepts.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				
				AllConcepts ac = new AllConcepts(line);
			
				if(ac.getConceptPath().toLowerCase().contains("vcf sample id")) acs.put(ac.getTvalChar(), ac);
			}
			
		}
		System.out.println("VCF Sample Ids found " + acs.size());
		
		return acs;
	}

	private static Map<String, String> readVcfIndex() throws IOException {
		Map<String, String> map;
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "vcfIndex.tsv"))) {
			
			String line;
			buffer.readLine();
			while((line = buffer.readLine()) != null) {
			
				String[] arr = line.split("\t");
				
				String[] sampleids = arr[4].split(",");
				String[] partIds = arr[5].split(",");
				
				map = IntStream.range (0, sampleids.length).boxed()
						.collect(Collectors.toMap
								(i->sampleids[i],
								i->partIds[i],
								(existing, replacement) -> existing,
								TreeMap::new
								));
				
				return map;
				
				
			}
			
		}
		
		return null;
	}

}
