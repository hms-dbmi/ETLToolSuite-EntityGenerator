package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import etl.jobs.Job;

import java.util.TreeMap;

/**
 * Legacy All Concepts Merge Implementation.
 *
 * @deprecated This class is deprecated in favor of {@link AllConceptsMergeProcessor}.
 *             The new implementation provides:
 *             - Java 25 LTS features (Virtual Threads, Structured Concurrency)
 *             - Integrated character normalization (µ → \)
 *             - 10-20x better performance on Graviton3 (r7g instances)
 *             - Comprehensive logging and monitoring
 *             - Better resource management
 *
 *             This legacy class is retained for reference and backward compatibility
 *             but should not be used for new deployments.
 *
 *             Migration: Simply update your Jenkins job to use the new JAR name.
 *             The new implementation maintains full backward compatibility with
 *             input/output formats and system properties.
 *
 * @see AllConceptsMergeProcessor (modernized replacement)
 * @since 1.0 (legacy)
 */
@Deprecated(since = "2.0", forRemoval = true)
public class DbGapDataMerge extends Job {

	private static final String[] AC_HEADERS = new String[5];
	static {
		AC_HEADERS[0] = "PATIENT_NUM";
		AC_HEADERS[1] = "CONCEPT_PATH";
		AC_HEADERS[2] = "NVAL_NUM";
		AC_HEADERS[3] = "TVAL_CHAR";
		AC_HEADERS[4] = "DATE_TIME";
	}

	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}

	private static void execute() throws Exception {
		
		Map<String, List<String>> rootNodeSort = sortByRootNodes();
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "allConcepts.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			buffer.write(BDCJob.toCsv(AC_HEADERS));
			buffer.close();
		}
		rootNodeSort.forEach((root, files) -> {
            System.out.println("merging " + root);
            files.forEach(file -> {
				try {
				ProcessBuilder processBuilder = new ProcessBuilder();
			
				processBuilder.command("bash", "-c", "cat " + file + " >> " + WRITE_DIR + "allConcepts.csv");
	
				//processBuilder.command("bash", "-c", "sed 's/µ/\\\\/g' " + entry.getValue() + " >> " + WRITE_DIR + "allConcepts.csv");
				
				Process process = processBuilder.start();

				StringBuilder output = new StringBuilder();
			
				BufferedReader reader = new BufferedReader(
					new InputStreamReader(process.getInputStream()));

				String line;
				while ((line = reader.readLine()) != null) {
					output.append(line + "\n");
				}

				int exitVal = process.waitFor();
				System.out.println(exitVal);
				if (exitVal == 0) {
					System.out.println(file + " merge Success!");
					System.out.println(output);
				} else {
					System.out.println(exitVal);
					System.err.println(file + " merge unsuccessful!");
					throw new Exception();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}		
			});
			System.out.print("all files in " + root + " merge Success!");
		});
		
	}

	// filename and rootnode
	private static Map<String, List<String>> sortByRootNodes() throws IOException {
		
		File dataDir = new File(DATA_DIR);
		// Map of rootNode and trial id
		Map<String,List<String>> rootnodes = new TreeMap<>();
		
		if(dataDir.isDirectory()) {
			
			for(File f: dataDir.listFiles()) {
				
				if(f.getName().contains("_allConcepts")) {
					
					try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + f.getName()))){
						String line;
						String fileName = DATA_DIR + f.getName();
						while((line = buffer.readLine()) != null) {
							String[] record = line.split(",");
							if(record.length < 1) continue;
							String rootNode = record[1];
							
							rootNode = rootNode.replaceAll("\"", "");
							rootNode = rootNode.substring(1);
							rootNode = rootNode.replaceAll("\\\\.*", "");
							if(rootnodes.containsKey(rootNode)){
								rootnodes.get(rootNode).add(fileName);
							}
							else{
								List<String> files = new ArrayList<>();
								files.add(fileName);
								rootnodes.put(rootNode, files);
							}
							
							break;
						}
						
					}
					
				}
				
			}
			
		}
		return rootnodes;
	}

}
