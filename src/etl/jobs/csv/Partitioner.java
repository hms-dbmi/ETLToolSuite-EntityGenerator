package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import etl.jobs.Job;
import etl.jobs.mappings.Mapping;

/**
 * @author Thomas DeSain
 * 
 * Will generate a mapping part file, config file and a sh script to execute the partition jobs
 * First iteration will separate all files.
 * Using algorithms in data evaluation should be able to create a more intelligent version of this after poc.
 *
 */

public class Partitioner extends Job {
			
	private static String CONFIG_FILE = "./resources/job.config";
	
	private static final String DELETE_ONLY = "N";
		
	private static String CONFIG_OUTPUT_DIR = "./resources/";
	
	private static String MAPPING_OUTPUT_DIR = "./mappings/";
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
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
		
		List<Mapping> mappingFile = Mapping.generateMappingListForHPDS(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		// map
    	// key = concept path
    	// value = set of mapping key
		Map<String,Set<Mapping>> mappings = new TreeMap<String,Set<Mapping>>();
		
		for(Mapping m: mappingFile) {
			if(mappings.containsKey(m.getRootNode())){
				 
				mappings.get(m.getRootNode()).add(m);
 			} else {
 				mappings.put(m.getRootNode(), new HashSet<Mapping>(Arrays.asList(m)));
 			}
		}
		
        try (BufferedReader input = Files.newBufferedReader(Paths.get(CONFIG_FILE))) {
        	
            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

    			int partition = 1;
    			
    			MAPPING_FILE = prop.getProperty("mappingfile");
    			
    			checkDataTypes(mappings);
    			
    			for(Entry<String,Set<Mapping>> entry: mappings.entrySet()) {
    				/*
    				ConfigFile cf = new ConfigFile(prop);
    				
                cf.mappingfile = "./mappings/mapping.part" + partition + ".csv";
                
                cf.appending = "Y";

                cf.ispartition = "Y";
                
                cf.finalpartition = "N";
                
                cf.mappingquotedstring = MAPPING_QUOTED_STRING;
             
                cf.sequencedata = "Y";
                
                cf.conceptcdstartseq = new Integer(currentConceptSeq).toString();
                
                cf.encounternumstartseq= new Integer(1).toString();
                
                //cf.patientnumstartseq  = new Integer(3).toString();
                
                cf.trialid = TRIAL_ID;
                
                cf.skipmapperheader = "N";
                
                cf.writedir = PROCESSING_FOLDER + partition + '_';
               */
    				String jobConfig = buildJobConfig(partition, prop);
    				
	                try(OutputStream output = new FileOutputStream(MAPPING_OUTPUT_DIR + "/mapping.part" + partition + ".csv")) {
	                		for(Mapping newmapping: entry.getValue()) {
	                			
	                			output.write((newmapping.toCSV() + '\n').getBytes());
	                		}
	                		output.flush();
	                		output.close();
	                		
	                }
	                
	                try(OutputStream output = new FileOutputStream(CONFIG_OUTPUT_DIR + "/config.part" + partition + ".config")) {
	                	
	    	            		output.write(jobConfig.getBytes());
	    	            		output.flush();
	    	            		output.close();
	            		
	                }

                //Iterate sequence for next partition in the loop
                //buffer is not enough setting to 50 to avoid conficts
                partition++;
    			}
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }		
	}
	private static void checkDataTypes(Map<String, Set<Mapping>> mappings) {

		
		for(Entry<String,Set<Mapping>> m1: mappings.entrySet()) {
			boolean mixedDataTypes = false;
			boolean isTval = false;
			boolean isNval = false;
			for(Mapping m2: m1.getValue()) {
				if(m2.getDataType().equalsIgnoreCase("text")) isTval = true;
				if(m2.getDataType().equalsIgnoreCase("numeric")) isNval = true;
			}
			if(isTval == isNval) mixedDataTypes = true;
			if(mixedDataTypes) {
				for(Mapping m2: m1.getValue()) {
					System.err.println("setting data type");
					m2.setDataType("TEXT");
				}
				for(Mapping m2: m1.getValue()) {
					System.out.println(m2.getRootNode());
					System.err.println(m2.getDataType());
					
				}
			}
		}
		
	}

	private static String buildJobConfig(int partition, Properties prop) {
		
		String startingSeq = prop.containsKey("patientnumstartseq") ? prop.getProperty("patientnumstartseq") : "-1";
		
		return "trialid=" + TRIAL_ID + "\n" + 
				"mappingfile=" + "./mappings/mapping.part" + partition + ".csv" + "\n" +
				"patientnumstartseq=" + startingSeq  + "\n" +
				"patientmappingfile=data/" + TRIAL_ID + "_PatientMapping.v2.csv" + "\n" +
				"usepatientmapping=" + "Y" + "\n" +
				"patientcol=" + "0" + "\n" +
				"writedir=" + PROCESSING_FOLDER + partition + '_';

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
		  File dir2 = new File(MAPPING_DIR);
		  directoryListing = dir2.listFiles();
		  if (directoryListing != null) {
		    for (File child : directoryListing) {
		      if(child.getName().matches("mapping\\.part.*\\.csv")) {
		    	  	child.delete();
		      }
		    }
		  } 	
	}
}
