package etl.jobs.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import etl.jobs.Job;
import etl.jobs.mappings.Mapping;
import etl.utils.Utils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

public class DataAnalyzer extends Job {
    private static List<Mapping> mappingsToRemove = new ArrayList<Mapping>();

    public static void main(String[] args) throws Exception {
        try {
            setVariables(args, buildProperties(args));
        } catch (Exception e) {
            System.err.println("Error processing variables");
            e.printStackTrace();
        }

        execute();

    }

    private static void execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {

        List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

        if (mappings.isEmpty()) System.err.println("NO MAPPINGS FOR " + TRIAL_ID);

        List<Mapping> newMappings = analyzeData(mappings);

        newMappings.removeAll(mappingsToRemove);
        try (BufferedWriter buffer = Files.newBufferedWriter(Paths.get("./logs/removed_mappings.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            Utils.writeToCsv(buffer, mappingsToRemove, MAPPING_QUOTED_STRING, MAPPING_DELIMITER);
        }

        try (BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "mapping.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            Utils.writeToCsv(buffer, newMappings, MAPPING_QUOTED_STRING, MAPPING_DELIMITER);
        }

    }

    private static List<Mapping> analyzeData(List<Mapping> mappings){
        List<Mapping> newMappings = new ArrayList<Mapping>();

        Map<String, List<Mapping>> mappingsMap = new HashMap<String, List<Mapping>>();
        Map<String, File> fileMap = new HashMap<String, File>();

        for (Mapping m : mappings) {

            String fileName = m.getKey().split(":")[0];
            Path path = Paths.get(DATA_DIR + fileName);
            if (Files.exists(path)) {


                if (mappingsMap.containsKey(fileName)) {

                    mappingsMap.get(fileName).add(m);


                } else {
                    System.out.println("Adding file " + fileName + "to file list");
                    mappingsMap.put(fileName, new ArrayList<Mapping>(Arrays.asList(m)));


                }
            }

        }
        System.out.println("Available processors: " + (ForkJoinPool.commonPool().getParallelism()));
        List<CompletableFuture<Void>> analyzerRuns = new ArrayList<CompletableFuture<Void>>();

        mappingsMap.forEach((key, value) -> {
            Path path = Paths.get(DATA_DIR + key);
            List<String[]> allRecs;
            try {
                BufferedReader buffer = Files.newBufferedReader(path);
                RFC4180ParserBuilder parserbuilder = new RFC4180ParserBuilder()
                        .withSeparator(DATA_SEPARATOR)
                        .withQuoteChar(DATA_QUOTED_STRING);

                RFC4180Parser parser = parserbuilder.build();

                CSVReaderBuilder builder = new CSVReaderBuilder(buffer)
                        .withCSVParser(parser);
                CSVReader csvreader = builder.build();
                if (SKIP_HEADERS) {
                    csvreader.readNext();
                }
                allRecs = csvreader.readAll();
                System.out.println("File " + path.getFileName() + " has " + allRecs.size() + " records");


            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            List<String[]> finalAllRecs = allRecs;
            value.forEach(m -> {

                CompletableFuture<Void> analyzerRun = CompletableFuture.runAsync(
                        () -> {

                            System.out.println("Starting analysis for " + m.getRootNode());
                            try {
                                int col = new Integer(m.getKey().split(":")[1]);
                                List<String> vals = new ArrayList<>();
                                finalAllRecs.forEach(rec -> {
                                    try {
                                        vals.add(rec[col].trim());
                                    }
                                    catch (ArrayIndexOutOfBoundsException e){
                                        System.out.println("Array out of bounds on " + Arrays.toString(rec) + " for col " + col + ". Array size is " + rec.length );
                                    }
                                });
                                newMappings.add(analyzeData(m, vals));
                            } catch (IOException e) {

                                throw new RuntimeException(e);
                            }
                            System.out.println("Analysis complete for " + m.getRootNode() + ". List now has " + newMappings.size() + " vars.");


                        });
                analyzerRuns.add(analyzerRun);

            });
        });
        CompletableFuture<Void> allRuns = CompletableFuture.allOf(analyzerRuns.toArray(new CompletableFuture<?>[0]));
        allRuns.join();
        System.out.println("Done data analyzer on " + newMappings.size() + " vars");

        return newMappings;

    }


    private static Mapping analyzeData(Mapping mapping, List<String> vals) throws IOException {
        Optional<String> nonnullText = vals.stream()
                .filter(val -> !(val == null)
                               && !val.isEmpty()
                               && !val.equalsIgnoreCase("null")
                               && !val.equalsIgnoreCase("na")
                               && !val.equalsIgnoreCase("n/a")
                               && !val.equalsIgnoreCase("nan")
                               && !val.equalsIgnoreCase("nil")
                               && !val.equalsIgnoreCase("nill") // Any additions to this list must be included in the GenerateAllConcepts.generateAllConcepts method
                               && !NumberUtils.isCreatable(val)).findFirst();
        if (nonnullText.isPresent()) {
            mapping.setDataType("TEXT");
            return mapping;
        } else {
            Optional<String> numeric = vals.stream().filter(NumberUtils::isCreatable).findFirst();
            if (numeric.isPresent()) {
                mapping.setDataType("NUMERIC");
            } else {
                System.out.println("No non-null values found! Removing mapping " + mapping.getKey() + " " + mapping.getRootNode());
                mapping.setDataType("TOREMOVE");
                mappingsToRemove.add(mapping);
            }


        }


        return mapping;

    }

}
