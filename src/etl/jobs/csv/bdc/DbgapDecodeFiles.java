package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import com.opencsv.*;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import etl.jobs.Job;

public class DbgapDecodeFiles extends Job {

    public static List<String> SAMPLE_ID = new ArrayList<String>() {{
        add("DBGAP_SAMPLE_ID");
        add("DBGAP SAMPID");
    }};

    public static List<String> SUBJECT_ID = new ArrayList<String>() {{
        add("dbGaP_Subject_ID".toUpperCase());
        add("dbGaP SubjID".toUpperCase());
    }};

    private static Map<String, String> SAMPLE_TO_SUBJECT_ID = new HashMap<>();

    // Global counters for exit status
    private static int studyWarnCount = 0;
    private static int fileFailureCount = 0;

    public static void main(String[] args) {
        try {
            // Processing variables can throw exceptions now, allowing us to see the error
            setVariables(args, buildProperties(args));
            execute();

            // Determine final exit code based on accumulated state
            if (fileFailureCount > 0) {
                System.err.println("Job completed with " + fileFailureCount + " critical file failures.");
                System.exit(1);
            } else if (studyWarnCount > 0) {
                System.err.println(studyWarnCount + " warnings detected. Review build log.");
                System.exit(255);
            } else {
                System.exit(0);
            }

        } catch (Exception e) {
            System.err.println("Fatal Job Error:");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void execute() throws IOException {
        if (!Files.isDirectory(Paths.get(DATA_DIR))) {
            System.err.println("Data directory not found: " + DATA_DIR);
            return;
        }

        File[] dataFiles = new File(DATA_DIR).listFiles();
        if (dataFiles == null) return;

        // Populate the lookup map before processing individual files
        setSampleToSubject();

        for (File data : dataFiles) {
            // Isolate errors per file so one bad file doesn't kill the whole job
            try {
                String[] fileNameArr = data.getName().split("\\.");
                if (!fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("txt")) {
                    continue;
                }

                String pht = BDCJob.getPht(fileNameArr);

                if (pht == null || pht.isEmpty()) {
                    continue;
                }

                File dictionaryFile = getDictFile(pht);

                if (dictionaryFile == null) {
                    System.err.println("Missing Data Dictionary for: " + data.getName() + " (pht: " + pht + ")");
                    fileFailureCount++;
                    continue;
                }

                studyWarnCount += translateData(data, dictionaryFile);

            } catch (Exception e) {
                System.err.println("CRITICAL FAILURE processing file: " + data.getName());
                e.printStackTrace();
                fileFailureCount++;
            }
        }
    }

    /**
     * Translates data using the dictionary.
     * Uses CSVReaderBuilder to avoid deprecation warnings.
     */
    private static int translateData(File data, File dictionaryFile) throws ParserConfigurationException, SAXException, IOException {
        int warnCount = 0;

        // Build dictionary first; if this fails, we throw exception immediately
        Document dataDic = buildDictionary(dictionaryFile);

        Map<String, String> headerLookup = new HashMap<>();
        Map<String, String> phvLookup = new HashMap<>();
        Map<String, Map<String, String>> valueLookup = buildValueLookup(dataDic);
        buildHeaderLookup(dataDic, headerLookup, phvLookup);

        // Configure the CSV Parser (Tab separator, 'µ' quote char as per original)
        CSVParser parser = new CSVParserBuilder()
                .withSeparator('\t')
                .withQuoteChar('µ')
                .build();

        // Try-with-resources guarantees streams close even if exceptions occur
        try (
                BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + data.getName()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(Paths.get(data.getAbsolutePath())))
                        .withCSVParser(parser)
                        .build()
        ) {
            String[] line;
            String[] headers = BDCJob.getHeaders(reader);

            if (headers != null) {
                System.out.println("Header row detected: " + Arrays.toString(headers));
                System.out.println("Header row count: " + headers.length);

                boolean isSampleId = SAMPLE_ID.contains(headers[0].toUpperCase());
                boolean hasDbgapSubjId = SUBJECT_ID.contains(headers[0].toUpperCase());
                String[] lineToWrite = new String[headers.length];

                if (hasDbgapSubjId || isSampleId) {
                    while ((line = reader.readNext()) != null) {
                        if (headers.length != line.length) {
                            // if the line length is off by one and the final value is blank it is likely there is an extra tab at the end of the file.
                            // so we will include the line.
                            if (!(headers.length == (line.length - 1) && StringUtils.isBlank(line[line.length - 1]))) {
                                System.err.println("Malformed row detected - skipping row inside " + data.getName());
                                System.err.println("Row data: " + Arrays.toString(line));
                                System.err.println("Row data count: " + line.length);
                                warnCount++;
                                continue; // Skip this row, don't crash
                            }
                        }

                        int colidx = 0;
                        for (String cell : line) {
                            if (headers.length - 1 < colidx) continue;

                            String header = headers[colidx];
                            if (valueLookup.containsKey(header)) {
                                Map<String, String> codedValues = valueLookup.get(header);
                                if (codedValues.containsKey(cell)) {
                                    cell = codedValues.get(cell);
                                }
                            }

                            lineToWrite[colidx] = cell;
                            colidx++;
                        }

                        if (isSampleId) {
                            lineToWrite[0] = findSampleToSubjectID(lineToWrite[0]);
                        }

                        // If mapping failed and returned null, skip writing this line
                        if (lineToWrite[0] == null) continue;

                        buffer.write(toCsv(lineToWrite));
                        buffer.flush();
                    }
                } else {
                    System.err.println("Missing dbgap subject id in file: " + data.getName());
                    // Note: If you want to delete the file on failure, you must do it AFTER this try block closes the writer.
                    warnCount++;
                }
            } else {
                System.err.println("Missing headers for " + data.getName());
                warnCount++;
            }
        } catch (IOException e) {
            // Rethrow so execute() can log it as a file failure
            throw new IOException("IO Error in translateData for file " + data.getName(), e);
        }

        return warnCount;
    }

    private static void setSampleToSubject() throws IOException {
        String[] sampleFiles = BDCJob.getStudySampleMultiFile();
        if (sampleFiles == null) return;

        for (String samplefile : sampleFiles) {
            int sampColId = -1;
            int subjColId = -1;

            // Note: BDCJob seems to handle basic file checks
            try {
                // Determine column indices
                for (String sampid : SAMPLE_ID) {
                    int idx = BDCJob.findRawDataColumnIdx(Paths.get(DATA_DIR + samplefile), sampid);
                    if (idx != -1) sampColId = idx;
                }

                for (String subjid : SUBJECT_ID) {
                    int idx = BDCJob.findRawDataColumnIdx(Paths.get(DATA_DIR + samplefile), subjid);
                    if (idx != -1) subjColId = idx;
                }

                if (subjColId == -1 || sampColId == -1) {
                    continue;
                }

                // Ensure reader is closed
                try (CSVReader reader = BDCJob.readRawBDCDataset(Paths.get(DATA_DIR + samplefile), true)) {
                    String[] line;
                    while ((line = reader.readNext()) != null) {
                        if (SUBJECT_ID.contains(line[subjColId].toUpperCase())) continue;
                        SAMPLE_TO_SUBJECT_ID.put(line[sampColId], line[subjColId]);
                    }
                }
            } catch (Exception e) {
                // Log but continue trying other sample files
                System.err.println("Error reading sample file: " + samplefile);
                e.printStackTrace();
            }
        }
    }

    private static String findSampleToSubjectID(String string) {
        if (SAMPLE_TO_SUBJECT_ID.containsKey(string)) {
            return SAMPLE_TO_SUBJECT_ID.get(string);
        }

        return null;
    }

    /**
     * Builds the XML Document. Throws exceptions instead of System.exit.
     */
    private static Document buildDictionary(File dictionaryFile) throws ParserConfigurationException, SAXException, IOException {
        if (dictionaryFile == null) {
            throw new IOException("Attempted to build dictionary from null file");
        }
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(dictionaryFile);
    }

    private static File getDictFile(String pht) {
        if (Files.isDirectory(Paths.get(DATA_DIR))) {
            File[] dataFiles = new File(DATA_DIR).listFiles();
            if (dataFiles == null) return null;

            for (File data : dataFiles) {
                if (!data.getName().contains("data_dict")) continue;
                String[] fileNameArr = data.getName().split("\\.");

                if (!fileNameArr[fileNameArr.length - 1].equalsIgnoreCase("xml")) {
                    continue;
                }

                if (pht.equalsIgnoreCase(BDCJob.getPht(fileNameArr))) {
                    return data;
                }
            }
        }
        return null;
    }

    private static void buildHeaderLookup(Document dataDic, Map<String, String> headerLookup, Map<String, String> phvLookup) {
        if (dataDic == null) return;

        NodeList variables = dataDic.getElementsByTagName("variable");
        for (int idx = 0; idx < variables.getLength(); idx++) {
            Node node = variables.item(idx);

            // Safety check for attributes
            if (node.getAttributes() == null || node.getAttributes().getNamedItem("id") == null) continue;

            NodeList variableChildren = node.getChildNodes();
            String id = node.getAttributes().getNamedItem("id").getNodeValue().split("\\.")[0];

            String name = "";
            String desc = "";
            for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
                Node node2 = variableChildren.item(idx2);
                if (node2.getNodeName().equalsIgnoreCase("name")) {
                    name = node2.getTextContent();
                    phvLookup.put(name, id);
                }

                if (node2.getNodeName().equalsIgnoreCase("description"))
                    desc = node2.getTextContent().replace('\"', '\'');
            }

            headerLookup.put(name, desc);
        }
    }

    private static Map<String, Map<String, String>> buildValueLookup(Document dataDic) {
        if (dataDic == null) return new HashMap<>();

        NodeList variables = dataDic.getElementsByTagName("variable");
        Map<String, Map<String, String>> valueLookup = new HashMap<>();

        for (int idx = 0; idx < variables.getLength(); idx++) {
            Node node = variables.item(idx);
            NodeList variableChildren = node.getChildNodes();
            String name = "";

            List<Node> valueNodes = new ArrayList<Node>();
            for (int idx2 = 0; idx2 < variableChildren.getLength(); idx2++) {
                Node node2 = variableChildren.item(idx2);
                if (node2.getNodeName().equalsIgnoreCase("name")) {
                    name = node2.getTextContent();
                }

                if (node2.getNodeName().equalsIgnoreCase("value")) {
                    valueNodes.add(node2);
                }
            }

            Map<String, String> valueMap = new HashMap<>();
            for (Node vnode : valueNodes) {
                String valueCodeName = findValueCodeName(vnode);

                // Safety check for empty value nodes
                if (vnode.getFirstChild() == null) {
                    continue;
                }

                String valueDecoded = vnode.getFirstChild().getNodeValue();
                String valueCoded;

                if (valueCodeName == null || vnode.getAttributes().getNamedItem(valueCodeName) == null) {
                    continue;
                } else {
                    valueCoded = vnode.getAttributes().getNamedItem(valueCodeName).getNodeValue();
                }

                valueMap.put(valueCoded, valueDecoded);
            }
            valueLookup.put(name, valueMap);
        }

        return valueLookup;
    }

    private static String findValueCodeName(Node vnode) {
        if (vnode.getAttributes() == null) return null;

        if (vnode.getAttributes().getNamedItem("code") != null) {
            return "code";
        }

        if (vnode.getAttributes().getNamedItem("value code") != null) {
            return "value code";
        }

        System.err.println("No value code provided in xml for " + vnode.getTextContent());
        return null;
    }
}