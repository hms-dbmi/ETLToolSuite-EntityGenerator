package etl.drivers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.opencsv.CSVReader;

import etl.job.entity.ConfigFile;
import etl.job.entity.Mapping;

/**
 * @author Thomas DeSain
 * 
 * Will generate a mapping part file, config file and a sh script to execute the partition jobs
 * First iteration will separate all files.
 * Using algorithms in data evaluation should be able to create a more intelligent version of this after poc.
 *
 */

public class Partitioner {
	
	private static final List<LinkOption> options = null;
	
	private static final boolean SKIP_HEADERS = true;

	private static String MAPPING_FILE = "./mappings/mapping.csv";

	private static final boolean MAPPING_SKIP_HEADER = false;

	private static final char MAPPING_DELIMITER = ',';

	private static final char MAPPING_QUOTED_STRING = '"';

	private static final String PATIENT_MAPPING_FILE = "./mappings/mapping.csv.patient";
	
	private static String CONFIG_FILE = "./resources/job.config";
	
	private static String EVALUATIONS_DIR = "./resources/";
	
	private static final String EVALUATION_FILE_CONCEPTS = EVALUATIONS_DIR + "conceptevaluation.txt";
	
	private static final String EVALUATION_FILE_FACTS = EVALUATIONS_DIR + "factevaluation.txt";

	private static final String DELETE_ONLY = "N";
	
	private static String DATA_DIR = "./data/";
	
	private static String CONFIG_OUTPUT_DIR = "./resources/";
	
	private static String MAPPING_OUTPUT_DIR = "./mappings/";
	
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}

		try {
			execute();
		} catch (InstantiationException | IllegalAccessException | IOException | CloneNotSupportedException e) {
			System.err.println(e);
		}
	}
	
	/**
	 * Partitions data and config file split into a record per mapping.
	 * This will be used to create multiple jobs that can be hyperthreaded.
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws CloneNotSupportedException
	 */
	private static void execute() throws InstantiationException, IllegalAccessException, IOException, CloneNotSupportedException {
		deleteOldConfigs();
		if(DELETE_ONLY.equalsIgnoreCase("Y")) return;
		
        try (InputStream input = new FileInputStream(CONFIG_FILE)) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

    			int partition = 1;
    			
    			int currentConceptSeq = 1;
    			
    			MAPPING_FILE = prop.getProperty("mappingfile");
    			
    			List<Mapping> mappingFile = Mapping.generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
    			
            for(Mapping m: mappingFile) {

            		Integer estimatedconcepts = getEstimatedConcepts(m.getKey());
            		if(estimatedconcepts == null ) {
            			System.err.println("No concepts for " + m.getKey());
            			continue;
            		}
            		Mapping newmapping = (Mapping) m.clone();
                ConfigFile cf = new ConfigFile(prop);
                
                cf.mappingfile = "./mappings/mapping.part" + partition + ".csv";
                
                cf.appending = "Y";
                cf.ispartition = "Y";
                cf.finalpartition = "N";
                cf.mappingquotedstring = '`';
             
                cf.sequencedata = "Y";
                cf.conceptcdstartseq = new Integer(currentConceptSeq).toString();
                cf.encounternumstartseq= new Integer(1).toString();
                cf.patientnumstartseq  = new Integer(3).toString();
            		
                try(OutputStream output = new FileOutputStream(MAPPING_OUTPUT_DIR + "/mapping.part" + partition + ".csv")) {
                	
                		output.write(newmapping.toCSV().getBytes());
                		output.flush();
                		output.close();
                		
                }
                try(OutputStream output = new FileOutputStream(CONFIG_OUTPUT_DIR + "/config.part" + partition + ".config")) {
                	
	            		output.write(cf.toString().getBytes());
	            		output.flush();
	            		output.close();
            		
                }

                //Iterate sequence for next partition in the loop
                //buffer is not enough setting to 10000 to avoid conficts
                currentConceptSeq = currentConceptSeq + estimatedconcepts + 10000;
                	partition++;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }		
	}
	/**
	 * deletes old partition configs
	 */
	private static void deleteOldConfigs() {
		  File dir = new File(CONFIG_OUTPUT_DIR);
		  File[] directoryListing = dir.listFiles();
		  if (directoryListing != null) {
		    for (File child : directoryListing) {
		      if(child.getName().matches("config\\.part.*\\.config")) {
		    	  	child.delete();
		      }
		    }
		  } 	
	}

	/**
	 * Reads the evaluation file to find the expected concept size for this mapping.
	 * This is important to properly sequence the data.
	 * 
	 * @param mappingKey
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static Integer getEstimatedConcepts(String mappingKey) throws FileNotFoundException, IOException {
		Path path = Paths.get(EVALUATION_FILE_CONCEPTS);
		try (CSVReader reader = new CSVReader(Files.newBufferedReader(path))) {
			Iterator<String[]> iter = reader.iterator();
			while(iter.hasNext()) {
				String[] row = iter.next();
				String k = row[0];

				if(k.equals(mappingKey)) {
					return new Integer(row[2]);
				}
			}
		}
		return null;
	}
	public static void setVariables(String[] args) throws Exception {
		for(String arg: args) {
			if(arg.equalsIgnoreCase( "-configfile" )){

				CONFIG_FILE = checkPassedArgs(arg, args);
				
			}
			if(arg.equalsIgnoreCase( "-evaluationsdir" )){
				
				EVALUATIONS_DIR = checkPassedArgs(arg, args);
				
			}
			if(arg.equalsIgnoreCase( "-configoutputdir" )){
				
				CONFIG_OUTPUT_DIR = checkPassedArgs(arg, args);
				
			}
			if(arg.equalsIgnoreCase( "-mappingoutputdir" )){
				
				MAPPING_OUTPUT_DIR = checkPassedArgs(arg, args);
				
			}			
			if(arg.equalsIgnoreCase( "-mappingoutputdir" )){
				
				MAPPING_OUTPUT_DIR = checkPassedArgs(arg, args);
				
			}
		}
		
		
		
	}
	
	
	// checks passed arguments and sends back value for that argument
	public static String checkPassedArgs(String arg, String[] args) throws Exception {
		
		int argcount = 0;
		
		String argv = new String();
		
		for(String thisarg: args) {
			
			if(thisarg.equals(arg)) {
				
				break;
				
			} else {
				
				argcount++;
				
			}
		}
		
		if(args.length > argcount) {
			
			argv = args[argcount + 1];
			
		} else {
			
			throw new Exception("Error in argument: " + arg );
			
		}
		return argv;

	}
}
