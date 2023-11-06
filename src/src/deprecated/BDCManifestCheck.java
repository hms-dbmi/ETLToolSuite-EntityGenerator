package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.opencsv.CSVReader;

import etl.jobs.Job;
import etl.jobs.jobproperties.JobProperties;

@Deprecated
public class BDCManifestCheck extends Job {

	
	
	private static Map<String, String> accessionlookup;

	private static List<Gen3manifest> manifest;

	private static Set<String> avillachPHSids;
	
	public static void main(String[] args) {
		
		try {
			
			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
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
		// Map containing patient mappings.
		// Key = phsAccession 
		// values = kv pair of dbgap id and hpds patient num
		accessionlookup = buildAccessionLookup();
		
		//Map<String, Map<String,String>> patientMappings = buildPatientMappings();
		
		manifest = buildMainfest();
		
		// unique patients across studies in manifest
		checkForUniquePatients();
		// collect phs ids generated for our data load
		avillachPHSids = buildavlids();
		// validate our ids against the release_manifest.
		validatePhsids();
		
	}

	private static void validatePhsids() throws IOException {
		Map<String,Set<String>> matchedids = new HashMap<>();
		Map<String,Set<String>> nonmatchedids = new HashMap<>();
		for(Gen3manifest g3: manifest) {
			if(avillachPHSids.contains(g3.datastage_subject_id)) {
				String phs = g3.study_accession.substring(0,9);
				if(matchedids.containsKey(phs)) {
					matchedids.get(phs).add(g3.datastage_subject_id);
				} else {
					matchedids.put(phs, new HashSet<String>( Arrays.asList(g3.datastage_subject_id) ));
				}
			} else {
				String phs = g3.study_accession.substring(0,10);
				if(nonmatchedids.containsKey(phs)) {
					nonmatchedids.get(phs).add(g3.datastage_subject_id);
				} else {
					nonmatchedids.put(phs, new HashSet<String>( Arrays.asList(g3.datastage_subject_id) ));
				}
			}
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "matchedids.txt"))) {
			
			for(Entry<String,Set<String>> entry: matchedids.entrySet()) {
				buffer.write(entry.getKey() + '=' + entry.getValue().size() + '\n');
			}
			
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "nonmatchedids.txt"))) {

			for(Entry<String,Set<String>> entry: nonmatchedids.entrySet()) {
				buffer.write(entry.getKey() + '=' + entry.getValue().size() + '\n');
			}
			
		}
		
	}

	private static Set<String> buildavlids() throws IOException {
		
		Set<String> ids = new HashSet<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "AccessionIds.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				if(line.length != 2) continue;
				ids.add(line[1]);
				
			}
			
		}
		
		return ids;
	}

	private static void checkForUniquePatients() throws IOException {
		Map<String,Set<String>> uniquePatients = new HashMap<>();
		
		for(Gen3manifest g3: manifest) {
			
			if(uniquePatients.containsKey(g3.study_accession)) {
				uniquePatients.get(g3.study_accession).add(g3.datastage_subject_id);
			} else {
				uniquePatients.put(g3.study_accession, new HashSet<String>(Arrays.asList(g3.datastage_subject_id)));
			}
			
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "release_manifest_patient_count.txt"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			for(Entry<String, Set<String>> entry : uniquePatients.entrySet()) {
				buffer.write(entry.getKey() + '=' + entry.getValue().size() + '\n');
			}
		}
	}

	private static List<Gen3manifest> buildMainfest() throws IOException {
		
		Stream<String> lines = Files.lines(Paths.get(DATA_DIR + "release_manifest.csv"));
		
		ArrayList<Gen3manifest> list = new ArrayList<>();
		
		lines.forEach(line -> {
			String col1 = line.split(",")[0].substring(1, 5);
			if(col1.equals("GUID")) {
				return;
			}
			BDCManifestCheck.Gen3manifest g3m = new BDCManifestCheck().new Gen3manifest(line.split(","));
			
			list.add(g3m);
			
		});
		
		return list;
	}

	private static Map<String,String> buildAccessionLookup() throws IOException {
		
		Stream<String> lines = Files.lines(Paths.get(DATA_DIR + "shortstudywithaccessions.tsv"));
		
		return lines.map(line -> line.split("\t")).collect(Collectors.toMap(line -> line[0], line -> line[1]));
	}

	private static Map<String, Map<String, String>> buildPatientMappings() throws IOException {
		
		Map<String, Map<String, String>> map = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(PATIENT_MAPPING_FILE))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
			
				if(map.containsKey(line[1])) {
					map.get(line[1]).put(line[0], line[2]);
				} else {
					Map<String,String> m = new HashMap<>();
					m.put(line[0], line[2]);
					map.put(line[1], m);
				}
				
			}
			
		}
		
		//return lines.map(line -> line.split(",")).collect(Collectors.toMap(line -> line[1], Collectors.toMap(line -> line[0], line -> line[2].replaceAll("\""))));
		return map;
	}
	public class Gen3manifest {
		public String GUID;
		public String submitted_sample_id;
		public String dbgap_sample_id;
		public String sra_sample_id;
		public String biosample_id;
		public String submitted_subject_id;
		public String dbgap_subject_id;
		public String consent_short_name;
		public String study_accession_with_consent;
		public String study_accession;
		public String study_with_consent;
		public String datastage_subject_id;
		public String consent_code;
		public String sex;
		public String body_site;
		public String analyte_type;
		public String sample_use;
		public String repository;
		public String dbgap_status;
		public String sra_data_details;
		public String file_size;
		public String md5;
		public String md5_hex;
		public String aws_uri;
		public String gcp_uri;
		public String permission;
		public String g_access_group;
		
		public Gen3manifest(String[] line) {
			//GUID = line[0];
			submitted_sample_id = line[1];
			dbgap_sample_id = line[2];
			//sra_sample_id = line[3];
			//biosample_id = line[4];
			submitted_subject_id = line[5];
			dbgap_subject_id = line[6];
			consent_short_name = line[7];
			//study_accession_with_consent = line[8];
			study_accession = line[9];
			//study_with_consent = line[10];
			datastage_subject_id = line[11];
			//consent_code = line[12];
			//sex = line[13];
			//body_site = line[14];
			//analyte_type = line[15];
			//sample_use = line[16];
			//repository = line[17];
			//dbgap_status = line[18];
			//sra_data_details = line[19];
			//file_size = line[20];
			//md5 = line[21];
			//md5_hex = line[22];
			//aws_uri = line[23];
			//gcp_uri = line[24];
			//permission = line[25];
			//g_access_group = line[26];
			
			
		}

		@Override
		public String toString() {
			return "Gen3manifest [GUID=" + GUID + ", submitted_sample_id=" + submitted_sample_id + ", dbgap_sample_id="
					+ dbgap_sample_id + ", sra_sample_id=" + sra_sample_id + ", biosample_id=" + biosample_id
					+ ", submitted_subject_id=" + submitted_subject_id + ", dbgap_subject_id=" + dbgap_subject_id
					+ ", consent_short_name=" + consent_short_name + ", study_accession_with_consent="
					+ study_accession_with_consent + ", study_accession=" + study_accession + ", study_with_consent="
					+ study_with_consent + ", datastage_subject_id=" + datastage_subject_id + ", consent_code="
					+ consent_code + ", sex=" + sex + ", body_site=" + body_site + ", analyte_type=" + analyte_type
					+ ", sample_use=" + sample_use + ", repository=" + repository + ", dbgap_status=" + dbgap_status
					+ ", sra_data_details=" + sra_data_details + ", file_size=" + file_size + ", md5=" + md5
					+ ", md5_hex=" + md5_hex + ", aws_uri=" + aws_uri + ", gcp_uri=" + gcp_uri + ", permission="
					+ permission + ", g_access_group=" + g_access_group + "]";
		}
	}
	protected static void setLocalVariables(String[] args, JobProperties properties) throws Exception {

	}
}
