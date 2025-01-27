package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.opencsv.CSVReader;

import etl.metadata.MetadataFactory;
import etl.metadata.bdc.BDCMetadata;
import etl.metadata.bdc.BDCMetadataElements;

public class UpdateCountsInJsonMetadata extends BDCJob {

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
		
		Map<String, String> phenoCounts = readPhenoCounts("Patient_Count_Per_Consents.csv");
		
		Map<String, String> genoCounts = readCounts(1,"genoCounts.csv");
		
		BDCMetadata meta = (BDCMetadata) MetadataFactory.readMetadata("BDC", new File(DATA_DIR + "metadata.json"));
		
		for(BDCMetadataElements element: meta.bio_data_catalyst) {
			updatePhenoCount(element,phenoCounts);
			updateGenoCount(element,genoCounts);
		}
		
		ObjectMapper mapper = new ObjectMapper();

		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		
		validate(meta);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "metadata.json") , StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			buffer.write(ow.writeValueAsString(meta));
			
		}
	}

	private static Map<String, String> readPhenoCounts(String dataFile) throws IOException {
		Map<String, String> counts = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + dataFile))) {
			
			try (CSVReader reader = new CSVReader(buffer)) {
				//reader.readNext();
				String[] line;
				while((line = reader.readNext()) != null) {
					counts.put(line[0], line[1]);
				}
			}
		}
		
		return counts;
	}

	private static void validate(BDCMetadata meta) {
		List<BDCMetadataElements> posCountMeta = new ArrayList<>();
		for(BDCMetadataElements element: meta.bio_data_catalyst) {
			if(element.raw_clinical_sample_size != -1 ) {
				if(!element.clinical_sample_size.equals(element.raw_clinical_sample_size)) {
					System.err.println(element.abbreviated_name + " - " + element.study_identifier + " - " + element.consent_group_code + " - Clinical Counts are off:");
					System.err.println("raw count = " + element.raw_clinical_sample_size);
					System.err.println("manual count = " + element.clinical_sample_size);
	
				}
			}
			if(element.raw_genetic_sample_size != -1) {
				if(!element.genetic_sample_size.equals(element.raw_genetic_sample_size)) {
					System.err.println(element.abbreviated_name + " - " + element.study_identifier + " - " + element.consent_group_code + " - Genomic Counts are off:");
					System.err.println("raw count = " + element.raw_genetic_sample_size);
					System.err.println("manual count = " + element.genetic_sample_size);
				}
			}
			if(element.clinical_sample_size != -1 ) {
				posCountMeta.add(element);
			}
		}
		meta.bio_data_catalyst = posCountMeta;
	}

	private static void updatePhenoCount(BDCMetadataElements element, Map<String, String> phenoCounts) {
		
		if(phenoCounts.containsKey(element.study_identifier + "." + element.consent_group_code) || phenoCounts.containsKey(element.study_identifier)) {
			if(!element.consent_group_code.equalsIgnoreCase("none") && !element.consent_group_code.equalsIgnoreCase("") && element.consent_group_code != null){
				System.out.println("Updating auth study counts for " + element.study_identifier + "." + element.consent_group_code);
				element.clinical_sample_size = new Integer(phenoCounts.get(element.study_identifier + "." + element.consent_group_code));
				element.clinical_variable_count = element.raw_clinical_variable_count;
			}
			else{
				System.out.println("Updating public study counts for " + element.study_identifier);
				element.clinical_sample_size = new Integer(phenoCounts.get(element.study_identifier));
				element.clinical_variable_count = element.raw_clinical_variable_count;
			}

		} else {
			
			System.err.println("missing pheno counts for " + element.study_identifier);
		}
		
	}

	private static void updateGenoCount(BDCMetadataElements element, Map<String, String> genoCounts) {
		
		if(genoCounts.containsKey(element.study_identifier + "." + element.consent_group_code)) {
			element.genetic_sample_size = new Integer(genoCounts.get(element.study_identifier + "." + element.consent_group_code));
		} else {
			if(element.consent_group_code.equals("c0")) { 
				System.out.println("ignoring c0 for genomic counts for " + element.study_identifier + "." + element.consent_group_code);
			} else {
				System.err.println("missing genomic counts for " + element.study_identifier + "." + element.consent_group_code);
			}
		}
		
	}
	
	private static Map<String, String> readCounts(int colIdx, String dataFile) throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + dataFile))) {
			
			
			@SuppressWarnings("resource")
			List<String[]> list = new CSVReader(buffer).readAll();
			
			Map<String,String> counts = new HashMap<>();
			
			for(String[] x: list) {
				counts.put(x[0], x[colIdx]);
			}
			return counts;
		}
		
	}

}
