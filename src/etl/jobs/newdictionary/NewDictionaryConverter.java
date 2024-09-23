package etl.jobs.newdictionary;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.core.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

import etl.jobs.csv.bdc.BDCJob;
import etl.jobs.jobproperties.JobProperties;

public class NewDictionaryConverter extends BDCJob {

    private static String SUBSET_DIR;
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

        File studiesFile = new File(SUBSET_DIR + STUDIES_META_FILE);
        getStudyConsents(studiesFile);
        File dictionaryFile = new File(SUBSET_DIR + DICTIONARY_FILE);
        getConcepts(dictionaryFile);

    }

    public static void getConcepts(File dictionaryFile) throws IOException {
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
                concept.setConceptPath(varMeta.get("columnmeta_hpds_path").asText().replace("\\\\", "\\"));

                if (concept.getConceptType().equalsIgnoreCase("continuous")) {
                    concept.setValues("[" + varMeta.get("columnmeta_min").asText() + ","
                            + varMeta.get("columnmeta_max").asText() + "]");
                } else {
                    concept.setValues(varMeta.get("values").asText());
                }
                if (name.startsWith("_")) {
                    // global var handling
                    concept.setParentConceptPath("\\global_variables\\");
                } else if (name.endsWith("All Variables")) {
                    // study doesnt have tables
                    concept.setParentConceptPath("\\" + studyMeta.get("columnmeta_study_id").asText() + "\\");
                    String studyId = studyMeta.get("columnmeta_study_id").asText();
                    if (!tableNames.contains(studyId)) {
                        tableNames.add(studyId);
                        Concept studyConcept = new Concept();
                        studyConcept.setConceptPath("\\" + studyId + "\\");
                        studyConcept.setDataset(variable.get("studyId").asText());
                        studyConcept.setConceptName(studyId);
                        studyConcept.setConceptType("categorical");
                        concepts.add(studyConcept);
                    }
                } else {
                    // study has tables
                    String tableId = variable.get("dtId").asText();
                    String studyId = studyMeta.get("columnmeta_study_id").asText();
                    concept.setParentConceptPath("\\" + studyId + "\\" + tableId + "\\");
                    if (!tableNames.contains(tableId)) {
                        tableNames.add(tableId);
                        Concept tableConcept = new Concept();
                        tableConcept.setConceptPath(concept.getParentConceptPath());
                        tableConcept.setDataset(variable.get("studyId").asText());
                        tableConcept.setConceptName(tableId);
                        tableConcept.setDisplayName(varMeta.get("derived_group_name").asText());
                        tableConcept.setConceptType("categorical");
                        tableConcept.setDescription(varMeta.get("derived_group_description").asText());
                        tableConcept.setParentConceptPath("\\" + studyId + "\\");
                        concepts.add(tableConcept);
                    }
                    if (!tableNames.contains(studyId)) {
                        tableNames.add(studyId);
                        Concept studyConcept = new Concept();
                        studyConcept.setConceptPath("\\" + studyId + "\\");
                        studyConcept.setDataset(variable.get("studyId").asText());
                        studyConcept.setConceptName(studyId);
                        studyConcept.setConceptType("categorical");
                        concepts.add(studyConcept);
                    }

                }
                concepts.add(concept);
            });

        });
        CSVWriter writer = new CSVWriter(new FileWriter(SUBSET_DIR + "concepts.csv"));
        concepts.forEach(concept -> {
            String[] entry = concept.getCsvEntry();
            writer.writeNext(entry);
        });
        writer.close();
    }

    public static void getStudyConsents(File studiesFile) throws IOException {

        ObjectMapper om = new ObjectMapper();
        CSVWriter writer = new CSVWriter(new FileWriter(SUBSET_DIR + "consents.csv"));
        JsonNode studiesTree = om.readTree(studiesFile);
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
                    String[] consentLine = { studyId, consentCode, description, participant_count, variable_count,
                            sample_count, authz };
                    writer.writeNext(consentLine);
                });
        writer.close();

    }

    private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--subsetDir")) {
                SUBSET_DIR = checkPassedArgs(arg, args);
            }

        }
    }
}
