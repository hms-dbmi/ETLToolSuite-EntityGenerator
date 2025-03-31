package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

/**
 * 
 * Purge any c0 patients from each studies allConcepts ( this will remove them
 * from global vars as well ).
 * remove any patient records whose study is not loaded from HRMN_allConcepts
 * and harmonized_consents
 * 
 * @author Tom
 *
 */
public class RemoveConsentZeroPatients extends BDCJob {

	private static final String GLOBAL_CONSENTS_PATH = "µ_consentsµ";

	private static Set<String> consentZeroPatientNums;

	private static int totCZpat = 0;
	private static final String[] AC_HEADERS = new String[5];
	static {
		AC_HEADERS[0] = "PATIENT_NUM";
		AC_HEADERS[1] = "CONCEPT_PATH";
		AC_HEADERS[2] = "NVAL_NUM";
		AC_HEADERS[3] = "TVAL_CHAR";
		AC_HEADERS[4] = "DATE_TIME";
	}

	public static void main(String[] args) {

		try {

			setVariables(args, buildProperties(args));

			// setLocalVariables(args, buildProperties(args));

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

		consentZeroPatientNums = readConsents(GLOBAL_CONSENTS_PATH);

		// build a hash set with only harmonized
		File datadir = new File("./beforeRemoval/");
		File[] files = datadir.listFiles(new FilenameFilter() {
			// apply a filter
			@Override
			public boolean accept(File dir, String name) {
				boolean result;
				if (name.contains("allConcepts")) {
					result = true;
				} else {
					result = false;
				}
				return result;
			}

		});
		System.out.println("Available processors: " + (ForkJoinPool.commonPool().getParallelism()));
		List<CompletableFuture<Void>> purgeRuns = new ArrayList<CompletableFuture<Void>>();
		for (int i = 0; i < files.length; i++) {
			final String fileName = files[i].getName();
			
			CompletableFuture<Void> purgeRun = CompletableFuture.runAsync(
					() -> {
						System.out.println("Thread started: " + fileName + "\n");
						try {
							
							purgePatients(fileName);
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
			purgeRuns.add(purgeRun);
		}
		CompletableFuture<Void> allRuns = CompletableFuture.allOf(purgeRuns.toArray(new CompletableFuture<?>[0]));
		allRuns.join(); // this line waits for all to be completed
		System.out.println("Done consent 0 removal");
		
	}

	private static void purgePatients(String allConceptsFile) throws IOException, InterruptedException {

		ProcessBuilder processBuilder = new ProcessBuilder();

		processBuilder.command("bash", "-c", "sed 's/µ/\\\\/g' " + ("./beforeRemoval/" + allConceptsFile) + " >> " + PROCESSING_FOLDER + allConceptsFile);

		Process process = processBuilder.start();
		int exitVal = process.waitFor();
		if (exitVal != 0) {
			System.out.println(exitVal);
			System.err.print(("./beforeRemoval/" + allConceptsFile) + " file format unsuccessful" + "\n");
		}
		List<String[]> allConceptsData;
		int presize;
		int postsize;
		BufferedReader br = Files.newBufferedReader(Paths.get(PROCESSING_FOLDER + allConceptsFile));

		try (CSVReader csvreader = new CSVReader(br, ',', '\"', 'µ')) {
			allConceptsData = csvreader.readAll();
			presize = allConceptsData.size();
			allConceptsData.removeIf(dataline -> {
				if (dataline[1].split("\\\\").length <= 1)
					return false;
				return consentZeroPatientNums.contains((dataline[0]));
			});
			postsize = allConceptsData.size();
			
			br.close();
			
		}
		//remove the intermediate file to save space
		Files.deleteIfExists(Paths.get(PROCESSING_FOLDER + allConceptsFile));

		try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(DATA_DIR + allConceptsFile), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			allConceptsData.forEach(outputline -> {
				try {
					bw.write(BDCJob.toCsv(outputline));
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			bw.flush();
			bw.close();
			System.out.print("Thread ended: " + (allConceptsFile) + ". Removed " + (presize-postsize) + " c0 subject related data points\n");
		}
	}

	private static Set<String> readConsents(String globalConsentsPath) throws IOException {

		Set<String> c0 = new HashSet<>();
		Set<String> cGood = new HashSet<>();
		try (BufferedReader reader = Files.newBufferedReader(Paths.get("./beforeRemoval/" + "GLOBAL_allConcepts.csv"))) {
			try (CSVReader csvreader = new CSVReader(reader)) {
				String[] line;
				while ((line = csvreader.readNext()) != null) {
					if (line[1].equals(GLOBAL_CONSENTS_PATH)) {
						line[0] = line[0].trim();
						if (line[3].contains("c0")) {
							c0.add(line[0].trim());
						} else {
							cGood.add(line[0].trim());
						}
					}
				}
			}

			for (String c : cGood) {
				if (c0.contains(c))
					c0.remove(c);
			}
			totCZpat = c0.size();

			System.out.println("Total consent 0 patients:" + c0.size());
			return c0;
		}
	}
}