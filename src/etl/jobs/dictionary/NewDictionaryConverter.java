package etl.jobs.dictionary;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import etl.jobs.csv.bdc.BDCJob;
import etl.jobs.jobproperties.JobProperties;

public class NewDictionaryConverter extends BDCJob {

    private static String SUBSET_DIR;
    private static Boolean IS_DCC = false;
    private static String STUDIES_META_FILE = "subset_metadata.json";
    private static String DICTIONARY_FILE = "subset_dictionary.json";

    public static void main(String[] args) {
        try {

            setLocalVariables(args, buildProperties(args));

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

        if (IS_DCC) {
            File dictionaryFile = new File(SUBSET_DIR + DICTIONARY_FILE);
            getConcepts(dictionaryFile);
        } else {
            File studiesFile = new File(SUBSET_DIR + STUDIES_META_FILE);
            getStudyConsents(studiesFile);
            File dictionaryFile = new File(SUBSET_DIR + DICTIONARY_FILE);
            getConcepts(dictionaryFile);
        }

    }

    public static void getConcepts(File dictionaryFile) throws IOException {
        System.out.println("Building concepts for file " + dictionaryFile.getAbsolutePath());
        ObjectMapper om = new ObjectMapper();
        JsonNode conceptTree = om.readTree(dictionaryFile);
        List<String> tableNames = new ArrayList<>();
        List<Concept> concepts = new ArrayList<>();

        conceptTree.fieldNames().forEachRemaining(name -> {
            JsonNode node = conceptTree.findValue(name);
            JsonNode variables = node.findValue("variables");
            JsonNode studyMeta = node.findValue("metadata");
            variables.forEach(variable -> {
                JsonNode varMeta = variable.get("metadata");
                Concept concept = new Concept();
                concept.setConceptName(variable.get("varId").asText());
                concept.setDataset(variable.get("studyId").asText());
                concept.setDisplayName(varMeta.get("columnmeta_name").asText());
                concept.setConceptType(varMeta.get("columnmeta_data_type").asText());
                concept.setDescription(varMeta.get("columnmeta_description").asText());
                concept.setStigmatized(varMeta.get("is_stigmatized").asBoolean());
                concept.setConceptPath(varMeta.get("columnmeta_hpds_path").asText().replace("\\", "\\\\"));

                if (concept.getConceptType().equalsIgnoreCase("continuous")) {
                    concept.setValues("[" + varMeta.get("columnmeta_min").asText() + ","
                            + varMeta.get("columnmeta_max").asText() + "]");
                } else {
                    JsonNode vals = variable.get("values");
                    try {
                        concept.setValues(om.writeValueAsString(vals));
                    } catch (JsonProcessingException e) {
                        
                        e.printStackTrace();
                    }
                }
                if (!name.startsWith("_")) {
                    // global var handling
                    
                    if (name.endsWith("All Variables")) {
                        // study doesnt have tables
                        concept.setParentConceptPath("\\\\" + studyMeta.get("columnmeta_study_id").asText() + "\\\\");
                        String studyId = studyMeta.get("columnmeta_study_id").asText();
                        if (!tableNames.contains(studyId)) {
                            tableNames.add(studyId);
                            Concept studyConcept = new Concept();
                            studyConcept.setConceptPath("\\\\" + studyId + "\\\\");
                            studyConcept.setDataset(variable.get("studyId").asText());
                            studyConcept.setConceptName(studyId);
                            studyConcept.setConceptType("categorical");
                            concepts.add(studyConcept);
                        }
                    } else {
                        // study has tables
                        String tableId = variable.get("dtId").asText();
                        String studyId = studyMeta.get("columnmeta_study_id").asText();
                        concept.setParentConceptPath("\\\\" + studyId + "\\\\" + tableId + "\\\\");
                        if (!tableNames.contains(tableId)) {
                            tableNames.add(tableId);
                            Concept tableConcept = new Concept();
                            tableConcept.setConceptPath(concept.getParentConceptPath());
                            tableConcept.setDataset(variable.get("studyId").asText());
                            tableConcept.setConceptName(tableId);
                            tableConcept.setDisplayName(varMeta.get("derived_group_name").asText());
                            tableConcept.setConceptType("categorical");
                            tableConcept.setDescription(varMeta.get("derived_group_description").asText());
                            tableConcept.setParentConceptPath("\\\\" + studyId + "\\\\");
                            concepts.add(tableConcept);
                        }
                        if (!tableNames.contains(studyId)) {
                            tableNames.add(studyId);
                            Concept studyConcept = new Concept();
                            studyConcept.setConceptPath("\\\\" + studyId + "\\\\");
                            studyConcept.setDataset(variable.get("studyId").asText());
                            studyConcept.setConceptName(studyId);
                            studyConcept.setConceptType("categorical");
                            concepts.add(studyConcept);
                        }

                    }
                    concepts.add(concept);
            }
            });

        });
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(SUBSET_DIR + "concepts.tsv"), CSVFormat.TDF)) {
            printer.printRecord("datasetRef", "name", "display", "conceptType", "conceptPath", "parentConceptPath",
                    "values",
                    "description", "stigmatized");
            concepts.sort((a, b) -> {
                return a.getConceptPath().compareTo(b.getConceptPath());
            });
            concepts.forEach(concept -> {
                ArrayList<String> entry = concept.getTsvEntry();
                try {
                    printer.printRecord(entry);
                } catch (IOException e) {
                    System.out.println("Could not print line");
                    e.printStackTrace();
                }
            });
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public static void getStudyConsents(File studiesFile) throws IOException {
        System.out.println("Building consents for file " + studiesFile.getAbsolutePath());
        ObjectMapper om = new ObjectMapper();
        CSVPrinter printer = new CSVPrinter(new FileWriter(SUBSET_DIR + "consents.tsv"), CSVFormat.TDF);
        JsonNode studiesTree = om.readTree(studiesFile);
        printer.printRecord("datasetRef", "consentCode", "description", "participantCount", "variableCount",
                "sampleCount",
                "authz");
        studiesTree.forEach(
                node -> {

                    String studyId = node.get("study_identifier").asText();
                    System.out.println(studyId);
                    String consentCode = node.get("consent_group_code").asText();
                    String description = node.get("consent_group_name").asText();
                    String participant_count = node.get("clinical_sample_size").asText();
                    String variable_count = node.get("clinical_variable_count").asText();
                    String sample_count = node.get("genetic_sample_size").asText();
                    String authz = node.get("authZ").asText();
                    try {
                        printer.printRecord(studyId, consentCode, description, participant_count, variable_count,
                                sample_count, authz);
                    } catch (IOException e) {
                        System.out.println("Could not print consent line");
                        e.printStackTrace();
                    }
                });
        printer.close();
    }

    private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--subsetDir")) {
                SUBSET_DIR = checkPassedArgs(arg, args);
            }
            if (arg.equalsIgnoreCase("--DCC")) {
                IS_DCC = true;
            }

        }
    }
}
