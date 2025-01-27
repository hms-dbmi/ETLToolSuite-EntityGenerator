package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.mappings.Mapping;

public class StudiesConsentsGenerator extends BDCJob {

	// generate a map that contains
	// <Study Abv name + phsIdentifier, < consent name, Set of patient nums >>

	/**
	 * Contains all the variants of consent columns in lowercase format
	 */


	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));

			// setLocalVariables(args, buildProperties(args));

		} catch (Exception e) {

			System.err.println("Error processing job options");
			e.printStackTrace();

		}

		try {
			execute();
		} catch (Exception e) {
			
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static void execute() throws IOException {
		// iterate over managed inputs
		List<BDCManagedInput> managedInputs = getManagedInputs();

		// gather patient mappings for all studies
		Map<String, Map<String, String>> patientMappings = getPatientMappings();

		Set<Mapping> mappings = new HashSet<Mapping>();

		Set<String> generatedMappings = new HashSet<>();

		mappings.add(new Mapping("rootNodeConsents.csv:1", "µ_studies_consentsµ", "", "TEXT", ""));

		generatedMappings.add("µ_studies_consentsµ");

		Set<String> rootNodePatients = new HashSet<>();

		for (BDCManagedInput input : managedInputs) {
			if (!input.getHasMulti().toLowerCase().equals("yes")){
				System.out.println(input.getStudyIdentifier() + " marked as not having subject multi. Skipping.");
				continue;
			}
			if (!input.getReadyToProcess().toLowerCase().equals("yes")){
				System.out.println(input.getStudyIdentifier() + " marked as not ready for ingest. Skipping.");
				continue;
			}
			String firstLevelName = "µ_studies_consentsµ" + input.getStudyIdentifier() + "µ";

			mappings.add(new Mapping(input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_first_level.csv:1",
					firstLevelName,
					"",
					"TEXT",
					""));

			generatedMappings.add(firstLevelName);

			Map<String, Set<String>> patSet = buildConsents(input.getStudyIdentifier(), input.getStudyAbvName(),
					patientMappings);

			Set<String> firstLevelPatients = new HashSet<>();

			for (Entry<String, Set<String>> entry : patSet.entrySet()) {

				if (entry.getKey().contains("Subjects did not participate in the study"))
					continue;

				String consentName = entry.getKey().replaceAll(".*\\(", "").replaceAll("\\).*", "").trim();

				String consentLevelName = firstLevelName + consentName + 'µ';

				if (!generatedMappings.contains(consentLevelName)) {

					mappings.add(new Mapping(
							input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_" + consentName + ".csv:1",
							consentLevelName,
							"",
							"TEXT",
							""));
					generatedMappings.add(consentLevelName);

				}

				rootNodePatients.addAll(entry.getValue());
				firstLevelPatients.addAll(entry.getValue());

				try (BufferedWriter writer = Files.newBufferedWriter(
						Paths.get(WRITE_DIR + input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_"
								+ consentName + ".csv"),
						StandardOpenOption.CREATE,
						StandardOpenOption.TRUNCATE_EXISTING)) {

					for (String patNum : entry.getValue()) {

						writer.write(toCsv(new String[] { patNum, "TRUE" }));

					}

				}
			}
			try (BufferedWriter writer = Files.newBufferedWriter(Paths
					.get(WRITE_DIR + input.getStudyAbvName() + "_" + input.getStudyIdentifier() + "_first_level.csv"),
					StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

				for (String patNum : firstLevelPatients) {

					writer.write(toCsv(new String[] { patNum, "TRUE" }));

				}

			}
		}

		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "rootNodeConsents.csv"),
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

			for (String patNum : rootNodePatients) {

				writer.write(toCsv(new String[] { patNum, "TRUE" }));

			}

		}
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "studies_consents_mapping.csv"),
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

			for (Mapping mapping : mappings) {

				writer.write(mapping.toCSV() + "\n");

			}

		}
	}

	private static Map<String, Set<String>> buildConsents(String studyIdentifier, String studyAbvName,
			Map<String, Map<String, String>> patientMappings) throws IOException {

		File dataDir = new File(DATA_DIR + "decoded/");

		Map<String, Set<String>> returnSet = new HashMap<>();

		if (dataDir.isDirectory()) {

			String[] fileNames = dataDir.list(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					if (name.startsWith(studyIdentifier) && name.toLowerCase().contains("subject.multi")
							&& name.toLowerCase().endsWith(".txt")) {
						return true;
					} else {
						return false;
					}
				}
			});

			if (fileNames.length > 1) {
				System.err.println("Expecting only one subject.multi file per study and found multiple, aborting : " +studyIdentifier);
				System.exit(255);
				//return returnSet;
				

			}
			if (fileNames.length == 0) {
				System.err.println("Expecting subject.multi file, none found, aborting : " +studyIdentifier);
				System.exit(255);
				//return returnSet;
				

			}
			try (BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "decoded/" + fileNames[0]))) {
				try (CSVReader reader = new CSVReader(buffer)) {
					int consentidx = getConsentIdx(fileNames[0]);

					if (consentidx != -1) {

						String[] line;

						while ((line = reader.readNext()) != null) {

							if (line.length < consentidx)
								continue;
							if (line[consentidx].isEmpty())
								continue;

							String hpds_id = mappingLookup(line[0], patientMappings.get(studyAbvName));
							if (hpds_id == null) {
								System.out.println("Potential issue - No HPDS ID found in + " + studyIdentifier + " for " + line[0]);
							}
							;

							if (returnSet.containsKey(line[consentidx])) {
								returnSet.get(line[consentidx]).add(hpds_id);
							} else {
								Set<String> set = new HashSet<>();
								set.add(hpds_id);
								returnSet.put(line[consentidx], set);
							}
						}

					} else {
						throw new IOException("Cannot find header for " + fileNames[0],
								new Throwable().fillInStackTrace());
					}
				}
			}

		} else {
			throw new IOException("parameter DATA_DIR = " + dataDir + " is not a directory!",
					new Throwable().fillInStackTrace());
		}
		System.out.println("Consents found in " + studyIdentifier + ": " + returnSet.keySet().toString());
		return returnSet;
	}


}
