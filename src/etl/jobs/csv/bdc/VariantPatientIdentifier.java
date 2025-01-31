package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.PatientMapping;

public class VariantPatientIdentifier extends BDCJob {

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
			e.printStackTrace();
		}	
	}
	
	private static void execute() throws IOException {

		List<BDCManagedInput> managedInputs = ManagedInputFactory.readManagedInput(METADATA_TYPE,MANAGED_INPUT);

		// read hail extract 
		List<HailExtract> extract = readHailDataExract();
		
		// read picsure ui data extract
		List<PicSureExtract> psExtract = readPicSureDataExtract();
		
		Set<String> nwdIds = getNwdIds(psExtract);	
		
		for(ManagedInput managedInput: managedInputs) {
			
			BDCManagedInput mi = (BDCManagedInput) managedInput;
			
			List<PatientMapping> patientMappings = PatientMapping.readPatientMappingFile(DATA_DIR + managedInput.getStudyAbvName().toUpperCase() + "_PatientMapping.v2.csv");

			Set<Integer> hpdsIds = PatientMapping.getHpdsPatIdKeySet(patientMappings);
			
			Set<String> idCount = new HashSet<>();
			for(PicSureExtract pe: psExtract) {
				
				if(mi.getStudyType().equals("TOPMED")) {
					
					if(pe.topmedStudyId.contains(mi.getStudyIdentifier())) {
						
						if(hpdsIds.contains(new Integer(pe.patientId))) {
							idCount.add(pe.patientId);
						} else {
							
							System.err.println("Patient Id does not belong in " + mi.getStudyIdentifier());
							System.err.println(pe.toString());
							
						}
						
					}
					
				} else if(mi.getStudyType().equals("PARENT")) {
					if(pe.parentStudyId.contains(mi.getStudyIdentifier())) {
						
						if(hpdsIds.contains(new Integer(pe.patientId))) {
							idCount.add(pe.patientId);
						} else {
							
							System.err.println("Patient Id does not belong in " + mi.getStudyIdentifier());
							System.err.println(pe.toString());
							
						}
						
					}
				}
				
			}
		}
		
		// get expected count per he input
		List<String[]> ltws = new ArrayList<>();

		for(HailExtract he: extract) {
			String[] ltw = new String[7];
			
			ltw[0] = he.locusContig;
			ltw[1] = he.locusPostion;
			ltw[2] = he.alleles[0];
			ltw[3] = he.alleles[1];
			ltw[4] = getCounts(he.het, nwdIds); 
			ltw[5] = getCounts(he.hom, nwdIds);
			ltw[6] = getAllCounts(he.het,he.hom, nwdIds);
			ltws.add(ltw);
		}
		try(BufferedWriter bw = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Variant_Counts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String[] ltw: ltws) {
				bw.write(toCsv(ltw));
			}
		}
		
	}

	private static String getAllCounts(String[] het, String[] hom, Set<String> nwdIds) {
		Set<String> combine = new HashSet<>();
		for(String s: het) {
			if(s.isEmpty()) continue;
			combine.add(s);
		}
		for(String s: hom) {
			if(s.isEmpty()) continue;
			combine.add(s);
		}
		String[] arr = Arrays.copyOf(combine.toArray(), combine.toArray().length, String[].class);
		return getCounts(arr,nwdIds);
	}

	private static String getCounts(String[] arr, Set<String> nwdIds) {
		int count = 0;
		for(String a: arr) {
			if(a.isEmpty()) continue;
			if(nwdIds.contains(a)) count++;
		}
		return new Integer(count).toString();
	}

	private static Set<String> getNwdIds(List<PicSureExtract> psExtract) {
		Set<String> set = new TreeSet<>();
		for(PicSureExtract pe : psExtract) {
			if(pe.vcfSampleId.isEmpty()) continue;
			set.add(pe.vcfSampleId);
		}
		return set;
	}

	private static List<PicSureExtract> readPicSureDataExtract() throws IOException {
		
		List<PicSureExtract> rs = new ArrayList<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "data_extract.csv"))) {
			
			try (CSVReader reader = new CSVReader(buffer)) {
				reader.readNext(); // skip header
				String[] line;
				
				while((line = reader.readNext()) != null) {
					
					PicSureExtract pe = new VariantPatientIdentifier().new PicSureExtract();
					//Patient ID,\_Parent Study Accession with Subject ID\,\_Topmed Study Accession with Subject ID\,\_VCF Sample Id\,\_consents\

					pe.patientId = line[0];
					pe.parentStudyId = line[1];
					
					pe.topmedStudyId = line[2];
					pe.vcfSampleId = line[3];
					pe.consents = line[4];
					
					rs.add(pe);
				}
			}
			
			return rs;
			
		}
				
	}

	private static List<HailExtract> readHailDataExract() throws IOException {
		
		List<HailExtract> rs = new ArrayList<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "chr1_10_21_22_partial_vcf_files_with_variants.tsv"))) {
			
			try (CSVReader reader = new CSVReader(buffer,'\t')) {
				reader.readNext(); // skip header
				String[] line;
				while((line = reader.readNext()) != null) {
					
					HailExtract he = new VariantPatientIdentifier().new HailExtract();
					
					he.locusContig = line[0];
					he.locusPostion = line[1];
					
					String alleles = line[2].substring(1,line[2].length()-1).replaceAll("['\\s]", "");
					String het = line[3].isEmpty() ? "" : line[3].substring(1,line[3].length()-1).replaceAll("['\\s]", "");
					String hom = line[4].isEmpty() ? "" : line[4].substring(1,line[4].length()-1).replaceAll("['\\s]", "");
					he.alleles = alleles.split(",");
					he.het = het.split(",");
					he.hom = hom.split(",");
					rs.add(he);
				}
			}
			
			return rs;
		}
	}

	
	public class HailExtract {
		
		public String locusContig;
		public String locusPostion;
		public String[] alleles = new String[2];
		public String[] het;
		public String[] hom;
		
		
	}
	public class PicSureExtract {
		//Patient ID,\_Parent Study Accession with Subject ID\,\_Topmed Study Accession with Subject ID\,\_VCF Sample Id\,\_consents\
		
		public String patientId;
		public String parentStudyId;
		public String topmedStudyId;
		public String vcfSampleId;
		public String consents;
		@Override
		public String toString() {
			return "PicSureExtract [patientId=" + patientId + ", parentStudyId=" + parentStudyId + ", topmedStudyId="
					+ topmedStudyId + ", vcfSampleId=" + vcfSampleId + ", consents=" + consents + "]";
		}
		
		
		
	}

}
