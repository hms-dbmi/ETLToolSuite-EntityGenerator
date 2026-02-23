package etl.jobs.csv.bdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import etl.jobs.mappings.Mapping;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class GenericMappingGenerator extends BDCJob {

	protected static boolean HASDATATABLES = false;
	protected static String METADATA_FILE;

	public static void main(String[] args) throws Exception {
		try {
			setVariables(args, buildProperties(args));
			setClassVariables(args);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace(System.err);
			System.exit(255);
		}

		try {
			execute();
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(255);
		}
	}

	private static void execute() throws Exception {
		Set<Mapping> mappings = buildMappings();
		if (mappings.isEmpty()) {
			System.err.println("Mappings for " + TRIAL_ID + " empty - verify your files and try again.");
			System.exit(255);
		}

		Path out = Paths.get(WRITE_DIR + TRIAL_ID + "_" + "mapping.csv");
		try (BufferedWriter writer = Files.newBufferedWriter(out,
				StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING)) {

			for (Mapping mapping : mappings) {
				writer.write(mapping.toCSV());
				writer.write('\n');
			}
		}
	}

	private static Set<Mapping> buildMappings() throws Exception {
		Set<Mapping> mappings = new TreeSet<>(Comparator.comparing(Mapping::getKey));

		Map<String, String> varMap = new HashMap<>();
		if (HASDATATABLES) {
			varMap = getPathMappings();
			if (varMap.isEmpty()) {
				System.err.println("Unable to build concept paths from metadata for " + TRIAL_ID + " - verify your files and try again.");
				System.exit(255);
			}
		}

		File dataDir = new File(DATA_DIR);
		if (!dataDir.isDirectory()) {
			System.err.println("DATA_DIR is not a directory: " + DATA_DIR);
			System.exit(255);
		}

		File[] files = dataDir.listFiles();
		if (files == null || files.length == 0) {
			System.err.println("No files found in DATA_DIR: " + DATA_DIR);
			return mappings;
		}

		for (File f : files) {
			String name = f.getName();

			if (name.startsWith("._")) {
				continue;
			}

			boolean isCsv = name.toLowerCase().endsWith(".csv");
			boolean isTxt = name.toLowerCase().endsWith(".txt");
			if (!isCsv && !isTxt) {
				continue;
			}

			System.out.println("Generating mapping for: " + name);
			System.out.println("Processing mappings for: " + name + " with datatable format set to " + HASDATATABLES);

			String[] headers;
			if (isTxt) {
				headers = getFixedHeadersForMultiTxt(name);
				if (headers == null) {
					// Treat other .txt files as CSV-like (first row is header)
					headers = readCsvHeader(Paths.get(DATA_DIR, name), name);
				}
			} else {
				headers = readCsvHeader(Paths.get(DATA_DIR, name), name);
			}

			if (headers == null || headers.length == 0) {
				System.err.println(name + " has an issue with its headers.");
				continue;
			}

			addHeaderMappings(headers, name, varMap, mappings);
		}

		return mappings;
	}

	private static String[] readCsvHeader(Path p, String name) {
		try (BufferedReader buffer = Files.newBufferedReader(p);
			 CSVReader reader = new CSVReader(buffer)) {
			return reader.readNext();
		} catch (Exception e) {
			System.err.println("Failed reading header for " + name + ": " + e.getMessage());
			return null;
		}
	}

	private static String[] getFixedHeadersForMultiTxt(String fileName) {
		String lower = fileName.toLowerCase();

		if (lower.endsWith("subject.multi.txt")) {
			return new String[]{"DBGAP_SUBJECT_ID", "SUBJECT_ID", "CONSENT"};
		}

		if (lower.endsWith("sample.multi.txt")) {
			return new String[]{"DBGAP_SUBJECT_ID", "DBGAP_SAMPLE_ID", "SUBJECT_ID", "SAMPLE_ID"};
		}

		return null;
	}

	private static void addHeaderMappings(String[] headers,
										  String fileName,
										  Map<String, String> varMap,
										  Set<Mapping> mappings) {

		int x = 0;
		for (String col : headers) {
			Mapping mapping = new Mapping();
			mapping.setKey(fileName + ":" + x);

			String rootNode;
			if (HASDATATABLES) {
				String key = (col == null ? "" : col).toLowerCase();
				String path = varMap.get(key);

				if (path == null) {
					rootNode = PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + col + PATH_SEPARATOR;
				} else {
					rootNode = path.replaceAll("^\"|\"$", "");
				}
			} else {
				rootNode = PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + col + PATH_SEPARATOR;
			}

			mapping.setRootNode(rootNode);
			mapping.setSupPath("");
			mapping.setDataType("TEXT");

			mappings.add(mapping);
			x++;
		}
	}

	private static Map<String, String> getPathMappings() throws Exception {
		File dictionaryFile = new File(METADATA_FILE);
		ObjectMapper om = new ObjectMapper();
		JsonNode dictTree = om.readTree(dictionaryFile);

		Map<String, String> varMap = new HashMap<>();
		System.out.println("Getting metadata paths from: " + dictionaryFile.getAbsolutePath());

		if (dictTree != null && dictTree.size() > 0 && dictTree.get(0).get("study_phs_number") != null) {
			System.out.println("Study id from file is " + dictTree.get(0).get("study_phs_number").asText());

			dictTree.get(0).path("form_group").elements().forEachRemaining(
					formGroup -> formGroup.path("form").elements().forEachRemaining(
							form -> form.path("variable_group").elements().forEachRemaining(
									varGroup -> varGroup.path("variable").elements().forEachRemaining(
											var -> {
												String varId = var.path("variable_id").asText().toLowerCase();
												String conceptPath = var.path("data_hierarchy").asText().replace("\\", PATH_SEPARATOR);
												varMap.put(varId, conceptPath);
											}
									)
							)
					)
			);

		} else if (dictTree != null && dictTree.size() > 0 && dictTree.get(0).get("dataset_ref") != null) {
			dictTree.elements().forEachRemaining(
					var -> {
						String varId = var.get("name").asText().toLowerCase();
						String conceptPath = var.get("concept_path").asText().replace("\\", PATH_SEPARATOR);
						varMap.put(varId, conceptPath);
					}
			);

		} else {
			throw new Exception("JSON Metadata does not fit either of the expected formats. Please review and try again.");
		}

		Path out = Paths.get(WRITE_DIR + TRIAL_ID + "_" + "concept_paths.csv");
		try (BufferedWriter writer = Files.newBufferedWriter(out,
				StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING)) {

			for (Map.Entry<String, String> e : varMap.entrySet()) {
				writer.write(e.getKey());
				writer.write(',');
				writer.write(e.getValue());
				writer.write('\n');
			}
		}

		return varMap;
	}

	private static void setClassVariables(String[] args) throws Exception {
		for (String arg : args) {
			if (arg.equalsIgnoreCase("-hasDatatables")) {
				HASDATATABLES = true;
			}
			if (arg.equalsIgnoreCase("-metadataFile")) {
				METADATA_FILE = checkPassedArgs(arg, args);
			}
		}
	}
}