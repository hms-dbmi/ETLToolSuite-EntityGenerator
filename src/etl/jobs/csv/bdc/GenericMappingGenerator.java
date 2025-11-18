package etl.jobs.csv.bdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import etl.jobs.mappings.Mapping;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class GenericMappingGenerator extends BDCJob {
	protected static boolean HASDATATABLES = false;
	protected static String METADATA_FILE;
	public static void main(String[] args) throws Exception {
		try {
			setVariables(args, buildProperties(args));
			setClassVariables(args);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		try {
			execute();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(e);
			
		}	
	}

	private static void execute() throws Exception {
		
		//ROOT_NODE = "PETAL Repository of Electronic Data COVID-19 Observational Study (RED CORAL) ( phs002363 )";
		//DATA_DIR = "./TEST/";
		Set<Mapping> mappings = buildMappings();
		if (mappings.isEmpty()){
			System.err.println("Mappings for " + TRIAL_ID + " empty - verify your files and try again.");
			System.exit(255);
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_" + "mapping.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			for(Mapping mapping:mappings) {
				writer.write(mapping.toCSV() + '\n');
				
			}
		}
		
	}

	private static Set<Mapping> buildMappings() throws Exception {
		Set<Mapping> mappings = new TreeSet<Mapping>(new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		Map<String, String> varMap = new HashMap<>();
		if(HASDATATABLES){
			varMap = getPathMappings();
			if (varMap.isEmpty()){
				System.err.println("Unable to build concept paths from metadata for " + TRIAL_ID + " - verify your files and try again.");
				System.exit(255);
			}
		}
		
		// read data dir
		File dataDir = new File(DATA_DIR);
		if(dataDir.isDirectory()) {
			
			for(File f: dataDir.listFiles()) {
				
				try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + f.getName()))) {
					if(!f.getName().contains(".csv")) continue;
					System.out.println("Generating mapping for: " + f.getName());
					try (CSVReader reader = new CSVReader(buffer)) {
						String[] headers = reader.readNext();
						int x = 0;

						if(f.getName().startsWith("._")){
							continue;
						}
						else{
							System.out.println("Processing mappings for: " + f.getName() + " with datatable format set to " + HASDATATABLES);
						}

						if(headers == null) {
							System.err.println(f.getName() + " has an issue with it's headers.");
							continue;
						}
						
						for(String col: headers) {
							
							Mapping mapping = new Mapping();
							
							mapping.setKey(f.getName() + ":" + x);
							if (HASDATATABLES){
								//if datatables enabled, gets the concept path from the metadata json file. When no mapping exists for varname, uses default path
								String path = varMap.get(col.toLowerCase());
								if (path == null && col.toLowerCase() != "dbgap_subject_id"){
									path = PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + col + PATH_SEPARATOR;
								}
								else {
									path = path.replaceAll("^\"|\"$", "");
								}
								mapping.setRootNode(path);
							}
							
							else{
								mapping.setRootNode(PATH_SEPARATOR + TRIAL_ID + PATH_SEPARATOR + col + PATH_SEPARATOR);
							}
							mapping.setSupPath("");
							mapping.setDataType("TEXT");
							
							mappings.add(mapping);
							
							x++;
							
						}
					}
					
				}
				
			}
			
		}
		return mappings;
	}

	private static Map<String, String> getPathMappings() throws Exception{
		File dictionaryFile = new File(METADATA_FILE);
        ObjectMapper om = new ObjectMapper();
        JsonNode dictTree = om.readTree(dictionaryFile);
        Map<String, String> varMap = new HashMap<>();
		System.out.println("Getting metadata paths from: " + dictionaryFile.getAbsolutePath());
		if(dictTree.get(0).get("study_phs_number") != null){
			//use original json format
	
		System.out.println("Study id from file is " + dictTree.get(0).get("study_phs_number").asText());
        dictTree.get(0).path("form_group").elements().forEachRemaining(
                formGroup -> {
                    formGroup.path("form").elements().forEachRemaining(
                            form -> {
                                    form.path("variable_group").elements().forEachRemaining(
                                            varGroup -> {
                                                varGroup.path("variable").elements().forEachRemaining(
                                                        var ->
                                                        {
                                                            String varId = var.path("variable_id").asText().toLowerCase();
                                                            String conceptPath = var.path("data_hierarchy").asText().replace("\\", PATH_SEPARATOR);
                                                            varMap.put(varId, conceptPath);
                                                        }
                                                );
                                            }
                                    );
                            }
                    );
                }
        );
	}
	else if (dictTree.get(0).get("dataset_ref") != null){
		//use simplified json format
		dictTree.elements().forEachRemaining(
			var -> {
				String varId = var.get("name").asText().toLowerCase();
				String conceptPath = var.get("concept_path").asText().replace("\\", PATH_SEPARATOR);
				varMap.put(varId, conceptPath);
			}
			

		);
	}
	else {
		throw new Exception("JSON Metadata does not fit either of the expected formats. Please review and try again.");
	}


		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_" + "concept_paths.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)) {
			varMap.forEach((varId, conceptPath) -> {
				try {
					writer.write(varId + ',' + conceptPath + '\n');
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			});
		}
		return varMap;

	}

	private static void setClassVariables(String[] args) throws Exception {
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-hasDatatables")) {
				HASDATATABLES= true;
			}
			if(arg.equalsIgnoreCase("-metadataFile")) {
				METADATA_FILE = checkPassedArgs(arg, args);
			}
		}
		
	}

}
