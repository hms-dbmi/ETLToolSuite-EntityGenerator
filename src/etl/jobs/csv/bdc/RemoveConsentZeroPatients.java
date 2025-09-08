package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

/**
 *
 * Purge patient data from specific studies where they have consent zero (c0).
 * This preserves patient data in studies where they have valid consent.
 *
 * Business rule: If an individual has a consent zero in a study they should not be
 * searchable/discoverable/accessible within that study. Other studies they with
 * non-consent zero should not be affected.
 *
 * Business Requirement: Backend data should remove records that pertain to only
 * the study with consent zero.
 *
 * @author Tom
 *
 */
public class RemoveConsentZeroPatients extends BDCJob {

    private static final String GLOBAL_CONSENTS_PATH = "µ_consentsµ";
    private static final String BEFORE_REMOVAL_DIR = "./beforeRemoval/";
    private static final String[] AC_HEADERS = {
            "PATIENT_NUM", "CONCEPT_PATH", "NVAL_NUM", "TVAL_CHAR", "DATE_TIME"
    };

    // Map to store patient consent information by study
    // Key: patientNum, Value: Map of studyId -> consentValue
    private static Map<String, Map<String, String>> patientConsentsByStudy;

    // Set of patient-study combinations with consent zero
    // Key format: "patientNum|phsXXXXXX"
    private static Set<String> consentZeroPatientStudies;

    private static int totCZpat = 0;

    public static void main(String[] args) {
        try {
            setVariables(args, buildProperties(args));
        } catch (Exception e) {
            System.err.println("Error processing variables: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        try {
            execute();
        } catch (IOException e) {
            System.err.println("Execution failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void execute() throws IOException {
        // Read consent data and build patient-study consent mapping
        readConsentsWithStudyPrecision(GLOBAL_CONSENTS_PATH);

        Path dataDir = Paths.get(BEFORE_REMOVAL_DIR);
        File[] files = dataDir.toFile().listFiles(
                (dir, name) -> name.contains("allConcepts")
        );

        if (files == null || files.length == 0) {
            System.out.println("No allConcepts files found in " + BEFORE_REMOVAL_DIR);
            return;
        }

        System.out.println("Available processors: " + ForkJoinPool.commonPool().getParallelism());

        int maxConcurrentThreads = Math.min(files.length, Runtime.getRuntime().availableProcessors());
        List<CompletableFuture<Void>> purgeRuns = new ArrayList<>(maxConcurrentThreads);

        for (File file : files) {
            final String fileName = file.getName();

            if (purgeRuns.size() >= maxConcurrentThreads) {
                CompletableFuture.anyOf(purgeRuns.toArray(new CompletableFuture[0])).join();
                purgeRuns.removeIf(CompletableFuture::isDone);
            }

            CompletableFuture<Void> purgeRun = CompletableFuture.runAsync(() -> {
                System.out.println("Thread started: " + fileName);
                try {
                    purgePatientsByStudy(fileName);
                } catch (Exception e) {
                    System.err.println("Error processing " + fileName + ": " + e.getMessage());
                    e.printStackTrace();
                }
            });
            purgeRuns.add(purgeRun);
        }

        CompletableFuture.allOf(purgeRuns.toArray(new CompletableFuture[0])).join();
        System.out.println("Done consent 0 removal");
    }

    private static void purgePatientsByStudy(String allConceptsFile) throws IOException, InterruptedException {
        Path inputPath = Paths.get(BEFORE_REMOVAL_DIR + allConceptsFile);
        Path processingPath = Paths.get(PROCESSING_FOLDER + allConceptsFile);
        Path outputPath = Paths.get(DATA_DIR + allConceptsFile);

        ProcessBuilder processBuilder = new ProcessBuilder(
                "bash", "-c",
                "sed 's/µ/\\\\/g' " + inputPath + " >> " + processingPath
        );

        Process process = processBuilder.start();
        int exitVal = process.waitFor();

        if (exitVal != 0) {
            System.err.println("File format unsuccessful for " + allConceptsFile + " (exit code: " + exitVal + ")");
            return;
        }

        AtomicInteger preCount = new AtomicInteger(0);
        AtomicInteger postCount = new AtomicInteger(0);

        try (BufferedReader br = Files.newBufferedReader(processingPath);
             CSVReader csvReader = new CSVReader(br, ',', '\"', 'µ');
             BufferedWriter bw = Files.newBufferedWriter(outputPath,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVWriter csvWriter = new CSVWriter(bw)) {

            String[] line;
            while ((line = csvReader.readNext()) != null) {
                preCount.incrementAndGet();

                boolean shouldKeep = true;
                if (line.length > 0 && line[0] != null) {
                    // Clean the patient number by removing quotes and trimming
                    String patientNum = line[0].trim().replaceAll("\"", "");

                    // Check if this record should be removed based on study-specific consent
                    if (shouldRemoveRecord(patientNum, line)) {
                        shouldKeep = false;
                        System.out.println("Removing c0 patient data for study-specific consent: " + patientNum + " from " + allConceptsFile);
                    }
                }

                if (shouldKeep) {
                    csvWriter.writeNext(line);
                    postCount.incrementAndGet();
                }
            }
        }

        Files.deleteIfExists(processingPath);

        int removedCount = preCount.get() - postCount.get();
        System.out.println("Thread ended: " + allConceptsFile + ". Removed " +
                removedCount + " c0 subject related data points");
    }

    /**
     * Determines if a record should be removed based on study-specific consent zero
     */
    private static boolean shouldRemoveRecord(String patientNum, String[] line) {
        if (line.length < 2) return false;

        // Extract study ID from concept path or text value
        String studyId = extractStudyId(line);

        if (studyId != null) {
            // Check if this patient has consent zero for this specific study
            String patientStudyKey = patientNum + "|" + studyId;
            return consentZeroPatientStudies.contains(patientStudyKey);
        }

        return false;
    }

    /**
     * Extract study ID (phsXXXXXX) from concept path or text value
     */
    private static String extractStudyId(String[] line) {
        // Check concept path (index 1)
        if (line.length > 1 && line[1] != null) {
            String studyId = extractPhsId(line[1]);
            if (studyId != null) return studyId;
        }

        // Check text value (index 3)
        if (line.length > 3 && line[3] != null) {
            String studyId = extractPhsId(line[3]);
            if (studyId != null) return studyId;
        }

        return null;
    }

    /**
     * Extract phsXXXXXX pattern from a string
     */
    private static String extractPhsId(String text) {
        if (text == null) return null;

        // Look for phs followed by digits pattern
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("phs\\d+");
        java.util.regex.Matcher matcher = pattern.matcher(text);

        if (matcher.find()) {
            return matcher.group();
        }

        return null;
    }

    private static void readConsentsWithStudyPrecision(String globalConsentsPath) throws IOException {
        patientConsentsByStudy = new HashMap<>();
        consentZeroPatientStudies = new HashSet<>();

        Path globalConceptsPath = Paths.get(BEFORE_REMOVAL_DIR + "GLOBAL_allConcepts.csv");

        try (BufferedReader reader = Files.newBufferedReader(globalConceptsPath);
             CSVReader csvreader = new CSVReader(reader)) {

            String[] line;
            while ((line = csvreader.readNext()) != null) {
                if (line.length > 3 && line[1] != null && GLOBAL_CONSENTS_PATH.equals(line[1])) {
                    // Clean the patient number by removing quotes and trimming
                    String patientNum = line[0] != null ? line[0].trim().replaceAll("\"", "") : "";

                    if (!patientNum.isEmpty() && line[3] != null) {
                        // Parse the consent value - format expected: phsXXXXXX.cX
                        String consentValue = line[3].trim();

                        if (consentValue.contains(".")) {
                            String[] parts = consentValue.split("\\.");
                            if (parts.length == 2) {
                                String studyId = parts[0]; // phsXXXXXX
                                String consent = parts[1]; // cX

                                // Store in patient consent mapping
                                patientConsentsByStudy.computeIfAbsent(patientNum, k -> new HashMap<>())
                                        .put(studyId, consent);

                                // If consent is c0, add to removal set
                                if ("c0".equals(consent)) {
                                    String patientStudyKey = patientNum + "|" + studyId;
                                    consentZeroPatientStudies.add(patientStudyKey);
                                }
                            }
                        }
                    }
                }
            }
        }

        totCZpat = consentZeroPatientStudies.size();

        System.out.println("Total consent 0 patient-study combinations: " + consentZeroPatientStudies.size());
        System.out.println("First 10 c0 patient-study combinations: ");
        consentZeroPatientStudies.stream().limit(10).forEach(combo -> System.out.println("  " + combo));

        // Additional debugging
        System.out.println("Sample consent data for debugging:");
        System.out.println("Patient consent mapping (first 5 patients): ");
        patientConsentsByStudy.entrySet().stream().limit(5).forEach(entry -> {
            System.out.println("  Patient " + entry.getKey() + ": " + entry.getValue());
        });
    }
}