package etl.jobs.csv;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.opencsv.CSVReader;

import etl.job.entity.Mapping;
import etl.job.entity.PatientMapping;


/**
 * This app is designed to analyze a data file and estimate 
 * the total number of facts, concepts and patients that will be 
 * generated.
 * 
 * This is a prerequisite step before partitioning can be performed.
 * 
 * @author Thomas DeSain
 *
 */
public class DataEvaluation extends Job{

	private static ArrayList<String> strings = new ArrayList<String>();
	
	private static ArrayList<String> cstrings = new ArrayList<String>();
	
	private static Map<String, Set<String>> uniqueVals = new HashMap<String, Set<String>>();
	
	private static HashMap<String, List<String>> totalVals;
	
	private static int totalFacts = 0;
	private static int totalConcepts = 0;
	private static Set<String> fileNames = new HashSet<String>();
	
	/**
	 * Main method that executes subprocesses
	 * Exception handling should happen here.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args);
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (InstantiationException e) {
			System.err.println(e);

		} catch (IllegalAccessException e) {
			System.err.println(e);

		} catch (IOException e) {
			System.err.println(e);

		}
	}


	/**
	 * Wrapper that calls subprocesses
	 * 
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private static void execute() throws IOException, InstantiationException, IllegalAccessException {
		cleanEvaluationFiles();
		
		List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		List<PatientMapping> patientMappingFile = !PATIENT_MAPPING_FILE.isEmpty() ? PatientMapping.class.newInstance().generateMappingList(PATIENT_MAPPING_FILE, MAPPING_DELIMITER): new ArrayList<PatientMapping>();
		
		Collections.sort(mappingFile, new Comparator<Mapping>() {

			@Override
			public int compare(Mapping o1, Mapping o2) {
				if(o1.getKey().split(":").length == 2 && o2.getKey().split(":").length == 2) {
					String o1fn = o1.getKey().split(":")[0];
					Integer o1col = new Integer(o1.getKey().split(":")[1]);
					
					String o2fn = o2.getKey().split(":")[0];
					Integer o2col = new Integer(o2.getKey().split(":")[1]);
					if(o1fn.equals(o2fn)) {
						
						if(o1col == o2col) return 0;
						else if(o1col < o2col) return 1;
						else return - 1; 
						
					} else {
						
						return 0;	
						
					}
				} else {
					return 0;
				}
			}
		});		
		Set<String> patientids = new HashSet<String>();

		for(PatientMapping pm :patientMappingFile) {
			if(pm.getPatientColumn().equalsIgnoreCase("patientnum")) {
				String pfile = pm.getPatientKey().split(":")[0];
				Integer pcol = new Integer(pm.getPatientKey().split(":")[1]);
				
				CSVReader reader = new CSVReader(Files.newBufferedReader(
						Paths.get(DATA_DIR + pfile)
						));
				if(SKIP_HEADERS) reader.readNext();
				Iterator<String[]> iter = reader.iterator();
				
				while(iter.hasNext()) {
					String[] e = iter.next();
					patientids.add(e[pcol]);
				}
			}
			
		}
		for(Mapping m: mappingFile) {
			fileNames.add(m.getKey().split(":")[0]);
		}

		Files.list(Paths.get(DATA_DIR)).forEach(path -> {		
			if(!Files.isDirectory(path))
			try {
				
				if(!fileNames.contains(path.getFileName().toString())) {
					System.out.println(path.getFileName() + " does not exist in the mapping file.");
					return;
				}
				List<Integer> nonOmittedColumns = new ArrayList<Integer>();

				for(Mapping m: mappingFile) {
					
					String fn = m.getKey().split(":")[0];

					if(!fn.equals(path.getFileName().toString())) continue;
					
					boolean isOmitted = m.getDataType().equalsIgnoreCase("omit");
					
					if(isOmitted) continue;

					Integer col = new Integer(m.getKey().split(":")[1]);
					
					nonOmittedColumns.add(col);
					
				}
	
				CSVReader reader = new CSVReader(Files.newBufferedReader(path));
							
				if(SKIP_HEADERS) reader.readNext();
						
				int nonnullvals = 0;
				
				Iterator<String[]> iter = reader.iterator();
				
				uniqueVals = new HashMap<String, Set<String>>();
				totalVals = new HashMap<String, List<String>>();
				
				while(iter.hasNext()) {
					int colIndex = 0;
					for(String v: iter.next()) {
						if(!nonOmittedColumns.contains(colIndex)) {
							colIndex++;
							continue;
						}
						if(v == null) {
							colIndex++;
							continue;
						}
						if(v.isEmpty()) {
							colIndex++;
							continue;
						}
						if(uniqueVals.containsKey(path.getFileName() + ":" + colIndex)) {
							uniqueVals.get(path.getFileName() + ":" + colIndex).add(v);
						} else {
							uniqueVals.put(path.getFileName() + ":" + colIndex, new HashSet<String>(Arrays.asList(v)));
						}
						if(totalVals.containsKey(path.getFileName() + ":" + colIndex)) {
							totalVals.get(path.getFileName() + ":" + colIndex).add(v);
						} else {
							totalVals.put(path.getFileName() + ":" + colIndex, new ArrayList<String>(Arrays.asList(v)));
						}					
						colIndex++;
					}
				}

				iter = reader.iterator();
														
				// filename and col with values
				Map<String, Integer> estimatedconcepts = new HashMap<String, Integer>();
				Map<String, Integer> estimatedfacts = new HashMap<String, Integer>();
				//slong estimatedconcepts = 0;
				for(Entry<String, Set<String>> entry: uniqueVals.entrySet()) {
					String dataType = getDataType(entry.getKey(),mappingFile);

					if(dataType.equalsIgnoreCase("text")) {
						estimatedconcepts.put(entry.getKey(), entry.getValue().size());
						totalConcepts += entry.getValue().size();

					} else {
						if(entry.getValue().isEmpty()) {
							estimatedconcepts.put(entry.getKey(), 0);
							
						} else {
							estimatedconcepts.put(entry.getKey(), 1);
							totalConcepts += 1;

						}
					}
					
					//estimatedconcepts += dataType.equalsIgnoreCase("text") ? entry.getKey() + '-' + entry.getValue().size(): 1;
				}
				
				for(Entry<String, List<String>> entry: totalVals.entrySet()) {
					estimatedfacts.put(entry.getKey(), entry.getValue().size());
					strings.add(entry.getKey() + " Facts = " + entry.getValue().size());
					totalFacts += entry.getValue().size();
					//estimatedconcepts += dataType.equalsIgnoreCase("text") ? entry.getKey() + '-' + entry.getValue().size(): 1;
				}				
				/*
				if(nonnullvals > 0 ) {
					strings.add(path.getFileName() + " Facts = " + estimatedFacts);
				}
								*/
		        try(OutputStream output = new FileOutputStream( WRITE_DIR + "conceptevaluation.txt", true)) {
		        		for(Entry<String, Integer> entry : estimatedconcepts.entrySet()) {
		        			String str2 = entry.getKey() + ",concepts," + entry.getValue() + "\n";	
		        			//output.write(str1.getBytes());
		        			output.write(str2.getBytes());

		        			output.flush();
		        		}
		        }

		        try(OutputStream output = new FileOutputStream(WRITE_DIR + "factevaluation.txt", true)) {
	        		for(Entry<String, Integer> entry : estimatedfacts.entrySet()) {
	        			String str2 = entry.getKey() + ",facts," + entry.getValue() + "\n";	
	        			//output.write(str1.getBytes());
	        			output.write(str2.getBytes());

	        			output.flush();
	        		}
	        }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		});
        try(OutputStream output = new FileOutputStream(WRITE_DIR + "dataevaluation.txt", true)) {
        		String fout = "Total expected facts: " + (totalFacts + patientids.size() ) + "\n\n";
        		String cout = "Total expected concepts: " + totalConcepts + "\n\n";
        		String pout = "Total expected patients: " + patientids.size() + "\n\n";
        		
    			output.write(fout.getBytes());
    			output.flush();
    			output.write(cout.getBytes());
    			output.flush();
    			output.write(pout.getBytes());
    			output.flush();
    			output.close();
        }
		
	}

	
	/**
	 * Method will clean all the previous evaluation files.
	 * 
	 * @throws IOException
	 */
	private static void cleanEvaluationFiles() throws IOException {
		OutputStream clean = new FileOutputStream(WRITE_DIR + "dataevaluation.txt");
		clean.close();
		clean = new FileOutputStream(WRITE_DIR + "factevaluation.txt");
		clean.close();
		clean = new FileOutputStream(WRITE_DIR + "conceptevaluation.txt");
		clean.close();
		clean = new FileOutputStream(WRITE_DIR + "dataevaluation.txt");
		clean.close();		
	}


	/**
	 * Iterates over a mappings and returns 
	 * 
	 * @param string
	 * @param mappingFile
	 * @return
	 */
	private static String getDataType(String string, List<Mapping> mappingFile) {
		for(Mapping m: mappingFile) {
			if(string.equals(m.getKey())) return m.getDataType();
		}
		return "";
	}
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void setVariables(String[] args) throws Exception {
		
		for(String arg: args) {
			if(arg.equalsIgnoreCase( "-patientmappingfile" )){
				
				PATIENT_MAPPING_FILE = checkPassedArgs(arg, args);
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
		}
	}
	
	
	// checks passed arguments and sends back value for that argument
	/**
	 * @param arg
	 * @param args
	 * @return
	 * @throws Exception
	 */
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