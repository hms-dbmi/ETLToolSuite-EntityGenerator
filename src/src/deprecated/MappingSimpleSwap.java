package src.deprecated;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import etl.job.entity.Mapping;
import etl.jobs.Job;

public class MappingSimpleSwap extends Job {

	public static void main(String[] args) throws IOException {
		List<Mapping> mappings = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		for(Mapping mapping: mappings) {
			String path = mapping.getRootNode();
			
			if(path.contains("/")) {
				path = path.replace("/","\\");
			} else if (path .contains("\\")) {
				path = path.replace("\\\\", "/");
			}
			
			mapping.setRootNode(path);
			
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(MAPPING_FILE + ".new"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			for(Mapping mapping: mappings) {
				writer.write(mapping.toCSV() + '\n');
			}
			writer.flush();
			writer.close();
		}
	}

}
