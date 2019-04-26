package etl.drivers;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.opencsv.CSVReader;

import etl.data.export.entities.ConfigFile;
import etl.job.jsontoi2b2tm.entity.Mapping;

/**
 * Will generate a mapping part file, config file and a sh script to execute the partition jobs
 * First iteration will separate all files.
 * Using algorithms in data evaluation should be able to create a more intelligent version of this after poc.
 * @author Tom
 *
 */
public class Partitioner {
	
	private static final List<LinkOption> options = null;
	
	private static final boolean SKIP_HEADERS = true;

	private static final String MAPPING_FILE = "./mappings/mapping.csv";

	private static final boolean MAPPING_SKIP_HEADER = false;

	private static final char MAPPING_DELIMITER = ',';

	private static final char MAPPING_QUOTED_STRING = '"';

	private static final String PATIENT_MAPPING_FILE = "./mappings/mapping.csv.patient";
	
	private static final String CONFIG_FILE = "./resources/MESA.config";

	private static final String EVALUATION_FILE_CONCEPTS = "./resources/conceptevaluation.txt";
	
	private static final String EVALUATION_FILE_FACTS = "./resources/factevaluation.txt";
	
	private static String DATA_DIR = "./data/";
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, IOException, CloneNotSupportedException {
		doMethod1();
	}

	private static void doMethod1() throws InstantiationException, IllegalAccessException, IOException, CloneNotSupportedException {
		List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);

		List<List<Mapping>> mappingfiles = new ArrayList<List<Mapping>>();
		// file name
		// label, value
		Map<String, Map<String, String>> evals = new HashMap<String, Map<String,String>>();
		
        try (InputStream input = new FileInputStream(CONFIG_FILE)) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

    			int partition = 100;
    			
    			int currentConceptSeq = 1;
    			
            for(Mapping m: mappingFile) {
            		Integer estimatedconcepts = getEstimatedConcepts(m.getKey());
            		if(estimatedconcepts == null) {
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
            		
                currentConceptSeq = currentConceptSeq + estimatedconcepts + 100;
                
                try(OutputStream output = new FileOutputStream("./mappings/mapping.part" + partition + ".csv")) {
                	
                		output.write(newmapping.toCSV().getBytes());
                		output.flush();
                		output.close();
                		
                }
                try(OutputStream output = new FileOutputStream("./resources/config.part" + partition + ".config")) {
                	
	            		output.write(cf.toString().getBytes());
	            		output.flush();
	            		output.close();
            		
                }
                	partition++;
            }
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }		
	}

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

}
