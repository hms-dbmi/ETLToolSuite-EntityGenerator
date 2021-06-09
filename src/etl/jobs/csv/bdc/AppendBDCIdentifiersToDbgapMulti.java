package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.ArrayUtils;

import com.opencsv.CSVReader;

public class AppendBDCIdentifiersToDbgapMulti extends BDCJob {

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
		// read multi file
		CSVReader reader = BDCJob.readRawBDCDataset(new File(DATA_DIR + "phs000007.v30.pht000182.v13.p11.Framingham_Subject.MULTI.txt"), true);
		
		List<String[]> dataExtract = readDataExtract();
		
		String[] line;
		
		List<String[]> toWriter = new ArrayList<>();

		
		Map<String,String> topmedlookup = buildLookup();
		
		String[] toWrite = new String[6];
		
		
		addHeaders(toWriter);
		
		while((line = reader.readNext()) != null) {
			toWrite = new String[6];
			toWrite[0] = line[0]; // dbgap subject id
			toWrite[1] = line[1]; // subject id
			toWrite[2] = line[5]; // subject source
			toWrite[3] = line.length >= 7 ? line[6]: ""; // source subject id
			toWrite[4] = "phs000007.v30_" + line[1]; // parent
			toWrite[5] = topmedlookup.containsKey(line[0]) ? "phs000974.v3_" + topmedlookup.get(line[0]) :"";
			toWriter.add(toWrite);
		}
		
		List<String[]> newExtract = buildNewExtract(dataExtract, toWriter);
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "FHS Identifers.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String[] arr: toWriter) {
				buffer.write(toCsv(arr));
			}
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "20201001_harmonized_BP_Baseline.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			for(String[] arr: newExtract) {
				buffer.write(toCsv(arr));
			}
		}
	}

	private static List<String[]> buildNewExtract(List<String[]> dataExtract, List<String[]> toWriter) {
		List<String[]> list = new ArrayList<>();
		for(String[] arr: dataExtract) {
			if(arr[0].equalsIgnoreCase("Patient ID")) {
				list.add(appendExtractHeaders(arr));
			} else {
				String studyAccessionId = arr[19].split("_")[1];
				
				String[] ids = getIds(studyAccessionId, toWriter);
				arr = ArrayUtils.add(arr, ids[0]);
				arr = ArrayUtils.add(arr, ids[1]);
				arr = ArrayUtils.add(arr, ids[2]);
				arr = ArrayUtils.add(arr, ids[3]);
				arr = ArrayUtils.add(arr, ids[4]);
				arr = ArrayUtils.add(arr, ids[5]);
				list.add(arr);
			}
		}
		return list;
	}

	private static String[] appendExtractHeaders(String[] arr) {
		arr = ArrayUtils.add(arr, "dbGap_Subject_ID");
		arr = ArrayUtils.add(arr, "SUBJECT_ID");
		arr = ArrayUtils.add(arr, "SUBJECT_SOURCE");
		arr = ArrayUtils.add(arr, "SOURCE_SUBJECT_ID");
		arr = ArrayUtils.add(arr, "_Parent Study Accession with Subject ID");
		arr = ArrayUtils.add(arr, "_Topmed Study Accession with Subject ID");
		return arr;
	}

	private static String[] getIds(String studyAccessionId, List<String[]> toWriter) {
		
		for(String[] arr: toWriter) {
			if(arr[0].equalsIgnoreCase("dbGap_Subject_ID")) continue;
			
			if(!arr[4].isEmpty() && arr[4].split("_")[1].equalsIgnoreCase(studyAccessionId)) return arr;
			if(!arr[5].isEmpty() && arr[5].split("_")[1].equalsIgnoreCase(studyAccessionId)) return arr;
		}
		
		return null;
	}

	private static List<String[]> readDataExtract() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "20201001_harmonized_BP_Baseline.csv"))) {
			CSVReader reader = new CSVReader(buffer, ',', '"', 'µ');
			
			return reader.readAll();
		}
		
	}

	private static void addHeaders(List<String[]> toWriter) {
		String[] toWrite = new String[6];
		toWrite[0] = "dbGap_Subject_ID";
		toWrite[1] = "SUBJECT_ID"; // subject id
		toWrite[2] = "SUBJECT_SOURCE"; // subject source
		toWrite[3] = "SOURCE_SUBJECT_ID"; // source subject id
		toWrite[4] = "_Parent Study Accession with Subject ID"; // parent
		toWrite[5] = "_Topmed Study Accession with Subject ID";		
		toWriter.add(toWrite);
	}

	private static Map<String, String> buildLookup() throws IOException {
		Map<String,String> map = new TreeMap<>();
		
		CSVReader reader = BDCJob.readRawBDCDataset(new File(DATA_DIR + "phs000974.v3.pht004909.v2.p2.TOPMed_WGS_FHS_Subject.MULTI.txt"), true);
		
		String[] line;
		
		while((line = reader.readNext()) != null) {
			
			map.put(line[0], line[1]);
			
		}
		return map;
	}

}
