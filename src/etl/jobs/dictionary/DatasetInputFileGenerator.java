package etl.jobs.dictionary;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;

import etl.etlinputs.managedinputs.ManagedInputFactory;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.csv.bdc.BDCJob;
import etl.jobs.jobproperties.JobProperties;

public class DatasetInputFileGenerator extends BDCJob {
    private static String SUBSET_DIR;
    private static String TARGET_REF;

    private static String STUDIES_META_FILE = "subset_metadata.json";

    public static void main(String[] args) {
        try {

            setLocalVariables(args, buildProperties(args));

        } catch (Exception e) {

            System.err.println("Error processing variables");

            System.err.println(e);

        }
        try {
            setVariables(args, buildProperties(args));

        } catch (Exception e) {

            System.err.println("Error processing variables");

            System.err.println(e);

        }

        try {
            execute();
        } catch (IOException e) {

            e.printStackTrace();
        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    private static void execute() throws IOException {
        getDataset();
        File studiesFile = new File(SUBSET_DIR + STUDIES_META_FILE);
        getConsents(studiesFile);

    }

    public static void getDataset() throws IOException {
        String[] header = { "ref", "full_name", "abbreviation", "description", "data_type", "study_type", "subject_types", "program_name", "version", "phase", "additional_info_link", "additional_info", "study_accession", "study_link", "request_access_text" };

        List<BDCManagedInput> managedInputs = ManagedInputFactory.readManagedInput(JOB_TYPE, MANAGED_INPUT);
        managedInputs.forEach(input -> {
            List<String[]> outputs = new ArrayList<>();
            outputs.add(header);
            String ref = input.getStudyIdentifier();
            if (ref.equals(TARGET_REF)) {
                System.out.println("Building dataset dictionary input file for " + ref);
                String full_name = input.getStudyFullName();
                String abbreviation = input.getStudyAbvName();
                String description = "";
                String data_type = input.getDataType();
                String study_type = input.getStudyType();
                String subject_types = input.getSubjectType();
                String program_name = input.getBdcPrograms();
                String version = input.getVersion();
                String phase = input.getPhase();
                String additional_info_link = input.getAdditionalInfoLink();
                String additional_info = input.getAdditionalInfo();
                String study_accession = input.getStudyIdentifier();
                if (!study_type.equals("PUBLIC")) {
                    study_accession = study_accession + "." + version + "." + phase;
                }
                String study_link = input.getStudyLink();
                String request_access_text = input.getRequestAccessText();
                String[] datasetInputs = { ref, full_name, abbreviation, description, data_type, study_type, subject_types, program_name, version, phase, additional_info_link, additional_info, study_accession, study_link, request_access_text };
                outputs.add(datasetInputs);
                File datasetOutput = new File(SUBSET_DIR + "/output/datasets.csv");
                try (PrintWriter pw = new PrintWriter(datasetOutput)) {
                    outputs.stream().map(line -> convertToCSV(line)).forEach(pw::println);
                } catch (FileNotFoundException e) {
                    System.err.println("File not found");
                }
            }

        }

        );
    }

    public static void getConsents(File studiesFile) throws IOException {
        System.out.println("Building consents for file " + studiesFile.getAbsolutePath());
        ObjectMapper om = new ObjectMapper();
        JsonNode studiesTree = om.readTree(studiesFile);
        String[] header = {"datasetRef", "consentCode", "description", "participantCount", "variableCount",
                "sampleCount",
                "authz"};
        List<String[]> outputs = new ArrayList<>();
        outputs.add(header);
        studiesTree.forEach(
                node -> {
                    String studyId = node.get("study_identifier").asText();
                    String consentCode = node.get("consent_group_code").asText();
                    String description = node.get("consent_group_name").asText();
                    String participant_count = "";
                    String variable_count = "";
                    String sample_count = "";
                    String authz = node.get("authZ").asText();
                    String[] consentInputs = {studyId, consentCode, description, participant_count, variable_count,
                        sample_count, authz};
                    outputs.add(consentInputs);
                    
                });
        File consentOutput = new File(SUBSET_DIR + "/output/consents.csv");
        try (PrintWriter pw = new PrintWriter(consentOutput)) {
            outputs.stream().map(line -> convertToCSV(line)).forEach(pw::println);
        } catch (FileNotFoundException e) {
                System.err.println("File not found");
        }
    }

    public static String convertToCSV(String[] data) {
        List<String> arr = Arrays.asList(data);
        return arr.stream()
                .map(d -> escapeSpecialCharacters(d))
                .collect(Collectors.joining(","));
    }

    public static String escapeSpecialCharacters(String data) {
        if (data == null) {
            return "";
        }
        String escapedData = data.replaceAll("\\R", " ");
        if (escapedData.contains(",") || escapedData.contains("\"") || escapedData.contains("'")) {
            escapedData = escapedData.replace("\"", "\"\"");
            escapedData = "\"" + escapedData + "\"";
        }
        return escapedData;
    }

    private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--subsetDir")) {
                SUBSET_DIR = checkPassedArgs(arg, args);
            }
            if (arg.equalsIgnoreCase("--ref")) {
                TARGET_REF = checkPassedArgs(arg, args);
            }

        }
    }
}
