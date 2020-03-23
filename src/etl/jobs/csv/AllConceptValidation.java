package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

public class AllConceptValidation extends Job {
	
	private static List<String> patientCounts = new ArrayList<>();
	
	private static List<String> conceptCounts = new ArrayList<>();
	
	private static List<String> patientIdConflicts = new ArrayList<>();
	
	/**
	 * Standalone Main so this class can generate concepts alone.
	 * @param args
	 */
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
		// Get Concept Count for each individual study that exists in the allConcepts.csv
		// Get Patient Count for Each study that exists in the allConcepts
		// Validate that studies have distinct patient sets that don't have id collisions.  
		// Excluding global variables and Harmonized data sets which should match ids in their correct study's
		Map<String,TreeSet<String>> uniqueIds = new HashMap<>();
		
		Map<String,String> patientCount = new HashMap<>();
		
		Map<String,Set<String>> conceptCount = new HashMap<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + "MergedAllConcepts.csv"))) {
			
			String line;
			
			String studyName = "";
			
			while((line = buffer.readLine()) != null) {
				
				String[] record = line.split(",");
				
				int x = 0;
				
				for(String cell: record) {
					if(cell.equals("\"\"")) continue;

					if(cell.length() == -1) continue;
					
					cell = StringUtils.chop(cell);
					
					if(cell.length() == -1 || cell.length() == 0) continue;

					cell = cell.substring(1);
					record[x] = cell;
					x++;
				}
				String currStudyName = record[1].substring(1).replaceAll("\\\\.*", "");
				
				if(!studyName.equals(currStudyName)) {
					System.out.println("Gathering ids and concepts for " + currStudyName);
					studyName = currStudyName;
				}
				
				putUniqueids(uniqueIds, record);
				
				buildConceptCount(record, conceptCount);
								
			}
			
		}
		
		runPatientCount(uniqueIds);

		runConceptCount(conceptCount);

		validateDistinctPatients(uniqueIds);
		
		printMessages();
	}

	private static void printMessages() throws IOException {
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "Validation Report.txt"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(String str: patientCounts) {
				buffer.write(str + '\n');
			}
			for(String str: conceptCounts) {
				buffer.write(str + '\n');
			}
			
		}
		
		try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "PatientConflicts.txt"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(String str: patientIdConflicts) {
				
				buffer.write(str + '\n');
				
			}
			
		}
		
	}

	private static void validateDistinctPatients(Map<String, TreeSet<String>> uniqueIds) {
		
		Collection<TreeSet<String>> patientIds = uniqueIds.values();
		
		String studyName = "";
		
		for(Entry<String, TreeSet<String>> entry: uniqueIds.entrySet()) {
			
			String currStudyName = entry.getKey();

			if(!studyName.equals(currStudyName)) {
				
				System.out.println("Validating Patient Ids for " + currStudyName);
				studyName = currStudyName;
				
			}
			
			if(entry.getKey().contains("\\DCC Harmonized data set\\")) continue;
			if(entry.getKey().subSequence(0, 2).toString().contains("\\_")) continue;

			TreeSet<String> currentPatients = entry.getValue();
			
			for(String currentPatient: currentPatients) {
				
				Iterator<TreeSet<String>> iter = patientIds.iterator();
				while(iter.hasNext()) {
					
					for(String patientId: iter.next()) {
						
						if(currentPatients.contains(patientId)) continue;
						
						if(currentPatient.equals(patientId)) {
							
							String patientConflict = "Bad id found in " + entry.getKey() + " having patient id " + currentPatient;
							
							patientIdConflicts.add(patientConflict);
							
						}
						
					}
					
				}
				
			}
			
		}
		
	}

	private static void runConceptCount(Map<String, Set<String>> conceptCount) {
		
		for(Entry<String,Set<String>> entry: conceptCount.entrySet()) {
			
			String message = " ( Concept Count ) " + entry.getKey() + " = " + entry.getValue().size();
			conceptCounts.add(message);

		}
		
	}

	private static void buildConceptCount(String[] record, Map<String, Set<String>> conceptCount) {
		
		String studyName = record[1].substring(1).replaceAll("\\\\.*", "");
		
		String concept = record[1];
		
		if(conceptCount.containsKey(studyName)) {
			
			Set<String> concepts = conceptCount.get(studyName);
			concepts.add(concept);
			conceptCount.put(studyName, concepts);
			
		} else {
		
			Set<String> concepts = new HashSet<String>();
			concepts.add(concept);
			conceptCount.put(studyName, concepts);
			
		}
		
	}

	private static void runPatientCount(Map<String, TreeSet<String>> uniqueIds) {

		for(Entry<String, TreeSet<String>> entry: uniqueIds.entrySet()) {
			
			String currStudyName = entry.getKey().substring(1).replaceAll("\\\\.*", "");
						
			String message = " ( Patient Count ) " + entry.getKey() + " = " + entry.getValue().size();
			
			patientCounts.add(message);
			
		}
		
	}

	private static void putUniqueids(Map<String, TreeSet<String>> uniqueIds, String[] record) {
		
		String studyName = record[1].substring(1).replaceAll("\\\\.*", "");
		
		if(uniqueIds.containsKey(studyName)) {
			
			TreeSet<String> ids = (TreeSet<String>) uniqueIds.get(studyName);
			ids.add(record[0]);
			uniqueIds.put(studyName, ids);
			
		} else {
			TreeSet<String> ids = new TreeSet<>();
			ids.add(record[0]);
			uniqueIds.put(studyName, ids);
		}
		
	}

}
