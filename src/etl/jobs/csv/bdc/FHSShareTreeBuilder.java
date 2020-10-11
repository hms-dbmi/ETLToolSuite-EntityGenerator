package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

public class FHSShareTreeBuilder extends Job {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2160314482947556166L;

	public static void main(String[] args) {
		try {

			setVariables(args, buildProperties(args));
							
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
			System.err.println(e);
		}
		
		
		try {
			
			execute();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void execute() throws IOException {
		// read nodes from share
		Map<String,List<String>> shareNodes = readShareNodes();
		
		List<Mapping> mappings = Mapping.generateMappingListForHPDS(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		List<Mapping> newmappings = new ArrayList<Mapping>();
		for(Entry<String,List<String>> entry: shareNodes.entrySet()) {
			// go through file names
			for(String fn: entry.getValue()) {
				File[] files = getFilesByFilename(fn);
				newmappings.addAll(getMatchedMappings(entry.getKey(),files,mappings));
				
			}
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "newMapping.csv"), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)){
			for(Mapping m : newmappings) {
				writer.write(m.toCSV() + '\n');
				writer.flush();
			}
		}
	}

	private static Set<Mapping> getMatchedMappings(String path, File[] files, List<Mapping> mappings) {
		
		Set<Mapping> matchedmappings = new HashSet<Mapping>();
		
		for(File f: files) {
			for(Mapping m: mappings) {
				if(m.getKey().split(":")[0].contains(f.getName())) {
					
					String[] rpath = m.getRootNode().substring(1).split("µ");
					
					StringBuilder np = new StringBuilder();
					np.append("µ");
					np.append(rpath[0]);
					np.append("µ");
					
					np.append(path);
					int x = 0;
					for(String p: rpath) {
						if(x == 0) {
							x++;
							continue;
						}
						np.append(p);
						np.append("µ");
						x++;
					}
					m.setRootNode(np.toString());
					System.out.println(m.toCSV());
					matchedmappings.add(m);
				}
			}
		}
		return matchedmappings;
	}

	private static File[] getFilesByFilename(String fn) {
		File dir = new File(DATA_DIR);
		File[] files = dir.listFiles(new FilenameFilter() {
		    @Override
		    public boolean accept(File dir, String name) {
		        return name.contains(fn);
		    }
		});
		return files;
	}

	private static Map<String, List<String>> readShareNodes() throws IOException {
		
		Map<String, List<String>> rmap = new HashMap<String,List<String>>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "FHS_sHARE_nodes.csv"))) {
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				List<String> filenames = new ArrayList<String>();
				if(line.length < 2) {
					continue;
				}
				filenames.addAll(Arrays.asList(line[1].split("\t")));
				rmap.put(line[0], filenames);
			}
			
		}
		
		return rmap;
		
	}

}
