package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.opencsv.CSVReader;

import etl.metadata.MetadataFactory;
import etl.metadata.bdc.BDCMetadata;
import etl.metadata.bdc.BDCMetadataElements;

public class UpdateMetadataCounts extends BDCJob {

	static Map<String,Map<String,String>> consentGroupPhenoCounts;
	static Map<String,Map<String,String>> consentGroupGenomicCounts;

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
		consentGroupPhenoCounts = buildPhenoCountsFromColumnMetadata();
		
		consentGroupGenomicCounts = buildGenomicCountsFromColumnMetadata();
		
		BDCMetadata meta = (BDCMetadata) MetadataFactory.readMetadata("BDC", new File(DATA_DIR + "metadata.json"));
		
		updateMetadataPhenoCounts(meta);
		
		
	}

	private static void updateMetadataPhenoCounts(BDCMetadata meta) {
		meta.bio_data_catalyst.forEach(metadata -> {
			if(consentGroupPhenoCounts.containsKey(metadata.study_identifier)) {
				if(consentGroupPhenoCounts.get(metadata.study_identifier).containsKey(metadata.consent_group_name_abv)) {
					metadata.clinical_sample_size = Integer.valueOf(consentGroupPhenoCounts.get(metadata.study_identifier).get(metadata.consent_group_name_abv));
				}
			}
		});
		
	}

	private static Map<String, Map<String, String>> buildGenomicCountsFromColumnMetadata() throws IOException {

		return null;
	}

	private static Map<String, Map<String, String>> buildPhenoCountsFromColumnMetadata() throws IOException {
		HashMap<String,Map<String,String>> consentGroups = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "columnMeta.csv"))) {
			CSVReader csvReader = new CSVReader(buffer, ',', '\"', 'µ');
			
			String[] line;
			while((line = csvReader.readNext()) != null ) {
				
				String[] path = line[0].split("\\\\");
				String rootNode = path[1];
				
				if(rootNode.equals("_studies_consents")) {
					
					if(path.length == 4) {
						
						String phs = path[2];
						String consentGroup = path[3];
						String count = line[10];
						
						if(!consentGroups.containsKey(phs)) {
							HashMap<String, String> m = new HashMap<String,String>();
							m.put(consentGroup, count);
							consentGroups.put(phs, m);
						} else {
							
							if(consentGroups.get(phs).containsKey(consentGroup)) {
								System.err.println("multiple entries for studies consent group! - " + phs + ":" + consentGroup );
							} else {
								consentGroups.get(phs).put(consentGroup, count);
							}
							
						}
					}
					
				}
				
			}
		}
		
		return consentGroups;
	}

}
