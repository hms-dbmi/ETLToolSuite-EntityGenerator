package etl.jobs.dictionary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import etl.jobs.csv.bdc.BDCJob;
import etl.jobs.jobproperties.JobProperties;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class ConceptInputFileGenerator extends BDCJob {
    private static String SUBSET_DIR;
    private static String TARGET_REF;
    private static String COLUMN_META_FILE = "subset_columnmeta.csv";

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
        String columnMetaFile = SUBSET_DIR + COLUMN_META_FILE;
        getConcepts(columnMetaFile);

    }

    private static void getConcepts(String columnMetaFile) throws IOException,JsonProcessingException {
        String[] headers = {"dataset_ref", "name", "display", "concept_type", "concept_path", "parent_concept_path",
                    "values",
                    "description", "stigmatized"};
        Set<Concept> concepts = new TreeSet<>( new Comparator<Concept>() {
            @Override
            public int compare(Concept o1, Concept o2) {
                if (o1.getConceptPath().length() == o2.getConceptPath().length()){
                    return o1.getConceptPath().compareTo(o2.getConceptPath());
                }
                return o1.getConceptPath().length() - (o2.getConceptPath().length());
            }
        });
        try (BufferedReader buffer = Files.newBufferedReader(Paths.get(columnMetaFile))) {
            CSVParser csvParser =  new CSVParserBuilder().withEscapeChar('φ').withQuoteChar('"').build();
            List<String[]> records = new CSVReaderBuilder(buffer).withCSVParser(csvParser).build().readAll();
            Set<String> parentConceptPaths = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            
            records.forEach(rec -> {
                Concept concept = new Concept();
                concept.setDataset(TARGET_REF);
                concept.setConceptPath(rec[0]);
                ObjectMapper om = new ObjectMapper();
                ArrayNode vals = om.createArrayNode();
                if (rec[3].toLowerCase().equals("true")){
                    concept.setConceptType("Categorical");
                    Arrays.asList(rec[4].split("µ")).forEach(val -> vals.add(val)); 
                }
                else{
                    concept.setConceptType("Continuous");
                    try{
                        Double min = Double.parseDouble(rec[5]);
                        Double max = Double.parseDouble(rec[6]);
                        vals.add(min);
                        vals.add(max);
                    }
                    catch (Exception e) {
                        System.err.println("Var marked as continuous but problem parsing min/max as double");
                        e.printStackTrace();
                    }
                    
                }
                try {
                    concept.setValues(om.writeValueAsString(vals));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                
                concept.setParentConceptPath(getParent(rec[0]));
                concept.setStigmatized(false);
                concepts.add(concept);
                parentConceptPaths.addAll(getAllParents(rec[0]));
            });
            parentConceptPaths.forEach(parentPath -> {
                Concept parent = new Concept();
                parent.setDataset(TARGET_REF);
                parent.setConceptPath(parentPath);
                parent.setConceptType("Categorical");
                parent.setParentConceptPath(getParent(parentPath));
                concepts.add(parent);
            });
            buffer.close();
        }
        File conceptsOutput = new File(SUBSET_DIR + "/output/concepts.csv");
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(conceptsOutput))) {
            csvWriter.writeNext(headers);
            concepts.stream().sorted( new Comparator<Concept>() {
            @Override
            public int compare(Concept o1, Concept o2) {
                if (o1.getConceptPath().length() == o2.getConceptPath().length()){
                    return o1.getConceptPath().compareTo(o2.getConceptPath());
                }
                return o1.getConceptPath().length() - (o2.getConceptPath().length());
            }
        }).map(Concept::getCsvEntry).forEach(csvWriter::writeNext);
        } catch (FileNotFoundException e) {
                System.err.println(SUBSET_DIR + "/output/ file directory not found");
        }
        
    }
    public static String getParent(String path){
        String parentPath = path.substring(0, path.lastIndexOf("\\"));
        parentPath = parentPath.substring(0, parentPath.lastIndexOf("\\")+1);
        if (parentPath.length()<2){
            parentPath = "";
        }
        return parentPath;
    }
    public static Set<String> getAllParents(String path){
        Set<String> parentPaths = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        while(! getParent(path).isEmpty()){
            path = getParent(path);
            parentPaths.add(path);
        }
        return parentPaths;
    }

    public static String convertToCSV(String[] data) {
        if (data == null || data.length < 1){
            return "";
        }
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
/*        if (escapedData.contains("\\")) {

            escapedData = escapedData.replaceAll("\\\\", "\\\\\\\\");

        }*/
        if (escapedData.contains(",") || escapedData.contains("\"") || escapedData.contains("'") ) {
            escapedData = escapedData.replace("\"", "\"\"");
            escapedData = "\"" + escapedData + "\"";
        }
        //System.out.println("data: " + data + " escaped: " + escapedData);
        return escapedData;
    }

    private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--ref")) {
                TARGET_REF = checkPassedArgs(arg, args);
            }
            else if (arg.equalsIgnoreCase("--subsetDir")) {
                SUBSET_DIR = checkPassedArgs(arg, args);
            }


        }
    }
}
