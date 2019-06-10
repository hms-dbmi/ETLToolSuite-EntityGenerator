package etl.drivers;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Set;

import com.opencsv.bean.CsvToBean;

import etl.job.entity.i2b2tm.ObservationFact;
import etl.utils.Utils;

public class CountGenerator {
	private static boolean SKIP_HEADERS = false;

	private static String DATA_DIR = "./completed/";
	
	private static final char DATA_SEPARATOR = ',';

	private static final char DATA_QUOTED_STRING = '"';
		
	private static String TRIAL_ID = "DEFAULT";
	
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void execute() throws IOException {
		// hashmap containing conceptcd and patientset
		HashMap<String, Set<String>> map = new HashMap<String,Set<String>>();
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + File.separatorChar + "ObservationFact.csv"))){

			CsvToBean<ObservationFact> csvToBean = Utils.readCsv(buffer, DATA_QUOTED_STRING, DATA_SEPARATOR);	
			
			csvToBean.forEach(fact -> {
				String patientNum = fact.getPatientNum();
				
			});
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

			if(arg.equalsIgnoreCase( "-datadir" )){
				DATA_DIR = checkPassedArgs(arg, args);
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
