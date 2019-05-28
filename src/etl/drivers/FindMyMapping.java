package etl.drivers;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVReader;

/**
 * simple application that will output all the concepts:
 * csv in this format
 * starting concept seq, root_path, data file name 
 * @author Tom
 *
 */
public class FindMyMapping {
	
	public static String CONFIG_DIR = "./resources/";
	
	public static String MAPPING_DIR = "./mappings/";
	private static OpenOption[] WRITE_OPTIONS = new OpenOption[] { WRITE, CREATE, APPEND };

	public static void main(String[] args) throws IOException {
		File configs = new File(CONFIG_DIR);
		
		File[] files = configs.listFiles();
		
		for(File f: files) {
			if(f.getName().contains("config.part")) {
				Properties prop = new Properties();
				
				prop.load(Files.newInputStream(f.toPath()));

				String mappingfile = prop.getProperty("mappingfile");
				CSVReader reader = new CSVReader(Files.newBufferedReader(Paths.get(mappingfile)));
				String[] mappingrec = reader.readNext();
				String startSeq = prop.getProperty("conceptcdstartseq");
				
				String datafilename = mappingrec[0];
				
				String dataconceptpath = mappingrec[1];
				
				String out = startSeq + "," + mappingfile + "," + datafilename + "," + dataconceptpath  + '\n';
					
				Files.write(Paths.get("findmypath.csv"), out.getBytes(), WRITE_OPTIONS);
				
				reader.close();
				
			}
			
		}
	}

}
