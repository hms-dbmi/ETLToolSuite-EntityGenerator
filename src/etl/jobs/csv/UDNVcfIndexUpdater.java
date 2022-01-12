package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.jobs.csv.bdc.BDCJob;

public class UDNVcfIndexUpdater extends BDCJob {

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
		Map<String,String> hpdsIdToUdnId = buildUdnIdsToHpds();
		
		List<String[]> vcfIndex = readVcfIndex();
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "vcfIndex.tsv"),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)){
			Collection<String> hpdsIds = hpdsIdToUdnId.values();
			Set<String> udnIds = hpdsIdToUdnId.keySet();
			for(String[] arr: vcfIndex) {
				if(arr[4].equals("sample_ids")) {
					writer.write(toTsv(arr));

					continue;
				}
				String vcfUdnIds = arr[4];
				System.out.println("Number of HPDS IDs in vcfIndex = " + vcfUdnIds.split(",").length);
				List<String> vcfHpdsIds = Arrays.asList(arr[5].split(","));
				
				List<String> goodUdnIds = new ArrayList<>();
				
				List<String> badUdnIds = new ArrayList<>();
				
				List<Integer> indexesToRemove = new ArrayList<>();
				
				int idIndex = 0;
				for(String id: vcfUdnIds.split(",")) {
					
					String udnIdOnly = id.split("-")[0];
					
					if(udnIds.contains(udnIdOnly)) {
						goodUdnIds.add(id);
					} else if(arr[4].equals("sample_ids")) {
						continue;
					} else {
						indexesToRemove.add(idIndex);
						badUdnIds.add(id);
						
					}
					idIndex++;
				}
				StringBuilder goodUdnIdsOnly = new StringBuilder();
				
				int x = 0;
				for(String gid: goodUdnIds) {
					
					goodUdnIdsOnly.append(gid);
					if(x != goodUdnIds.size() - 1 ) {
						
						goodUdnIdsOnly.append(",");
						
					}
						
				}
				
				StringBuilder goodHpdsIdsOnly = new StringBuilder();
				
				int y = 0;

				for(String gid: goodUdnIds) {
					
					String udnIdOnly = gid.split("-")[0];

					goodHpdsIdsOnly.append(hpdsIdToUdnId.get(udnIdOnly));
					
					if(y != goodUdnIds.size() - 1 ) {
						
						goodHpdsIdsOnly.append(",");
						
					}
					y++;
					/*
					if(!indexesToRemove.contains(hpdsId)) {
						goodHpdsIdsOnly.append(hpdsId);
						
						if(y != vcfHpdsIds.size() - 1 ) {
							
							goodUdnIdsOnly.append(",");
							
						}
					}
					y++;
					*/
				}
				arr[4] = goodUdnIdsOnly.toString();
				arr[5] = goodHpdsIdsOnly.toString();
				
				System.out.println("writing # UDN_IDS " + goodUdnIds.size());
				
				
				writer.write(toTsv(arr));
				
			}
		}
	}
	protected static char[] toTsv(String[] line) {
		StringBuilder sb = new StringBuilder();
		
		int lastNode = line.length - 1;
		int x = 0;
		for(String node: line) {
			
			//sb.append('"');
			sb.append(node);
			//sb.append('"');
			
			if(x == lastNode) {
				sb.append('\n');
			} else {
				sb.append('\t');
			}
			x++;
		}
		
		return sb.toString().toCharArray();
	}
	private static List<String[]> readVcfIndex() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "vcfIndex.tsv"))) {
			CSVReader reader = new CSVReader(buffer, '\t', '"', 'µ');		
			
			return reader.readAll();
		}
	}

	private static Map<String, String> buildUdnIdsToHpds() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "UDN_ID_EXTRACT.csv"))) {
			CSVReader reader = new CSVReader(buffer, ',', '"', 'µ');
			Map<String,String> map = new HashMap<>();
			String[] line;
			while((line = reader.readNext())!= null) {
				
				map.put(line[1], line[0]);
				
			}
			return map;

		}
		
	}

}
