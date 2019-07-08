package etl.drivers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import com.opencsv.bean.CsvToBean;

import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.TableAccess;
import etl.jobs.csv.Job;
import etl.utils.Utils;

/**
 * @author Thomas DeSain
 * Take metadata generated from metadatagenerator and replicates 
 * parent nodes to fill out metadata records.
 *
 */
public class FillInTree extends Job{
	
	private static boolean SKIP_HEADERS = false;

	private static int clevel = 1; 
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
	
	private static void execute() throws Exception {
		Set<I2B2> nodes = new HashSet<I2B2>();
		Set<TableAccess> tableAccess = new HashSet<TableAccess>();

		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){
			
			CsvToBean<I2B2> csvToBean = 
					Utils.readCsvToBean(I2B2.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
			nodes = new HashSet<I2B2>(csvToBean.parse());
			
			I2B2.fillTree(nodes, clevel + 1, clevel + 2);
			
			// build tableaccess entities
			tableAccess.addAll(TableAccess.createTableAccess(nodes));
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(
				Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){

			Utils.writeToCsv(buffer, nodes, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 		
		try(BufferedWriter buffer = Files.newBufferedWriter(
				Paths.get(WRITE_DIR + File.separatorChar + "TableAccess.csv"))){

			Utils.writeToCsv(buffer, tableAccess, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 		
	}

	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase("-skipheaders")){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					SKIP_HEADERS = true;
				} 
			}
			if(arg.equalsIgnoreCase( "-mappingskipheaders" )){
				String skip = checkPassedArgs(arg, args);
				if(skip.equalsIgnoreCase("Y")) {
					MAPPING_SKIP_HEADER = true;
				} 
			}
			if(arg.equalsIgnoreCase( "-mappingquotedstring" )){
				String qs = checkPassedArgs(arg, args);
				MAPPING_QUOTED_STRING = qs.charAt(0);
			}
			if(arg.equalsIgnoreCase( "-mappingdelimiter" )){
				String md = checkPassedArgs(arg, args);
				MAPPING_DELIMITER = md.charAt(0);
			}
			if(arg.equalsIgnoreCase( "-mappingfile" )){
				MAPPING_FILE = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-writedir" )){
				WRITE_DIR = checkPassedArgs(arg, args);
			} 
			if(arg.equalsIgnoreCase( "-trialid" )){
				TRIAL_ID  = checkPassedArgs(arg, args);
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
