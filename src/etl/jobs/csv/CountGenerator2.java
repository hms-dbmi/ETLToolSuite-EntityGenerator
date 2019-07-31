package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptCounts;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.utils.Utils;

/**
 * Generates Concept Counts Entity file.
 * 
 * It required that concept dimension and fact entities are generated first and
 * that they are located in the DATA_DIR directory.
 * 
 * @author Thomas DeSain
 *
 */
public class CountGenerator2 extends Job{

	/**
	 * Main method that executes subprocesses
	 * Exception handled should happen here.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (IOException e) {
			System.err.println(e);
		} catch (CsvDataTypeMismatchException e) {
			System.err.println(e);
		} catch (CsvRequiredFieldEmptyException e) {
			System.err.println(e);
		}
	}

	/**
	 * Wrapper that calls subprocesses
	 * 
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static void execute() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		// hashmap containing conceptcd and patientset
		File directory = new File(PROCESSING_FOLDER);
		if(!directory.exists()) directory.mkdir();
		System.out.println("Reading Facts.");
		readFactFile();
		System.out.println("Generating Counts.");
		generateCounts();

	}

	/**
	 * Generates the concept counts using the concept dimension file.
	 * Will recursively break down all paths into smaller nodes and use the factmap to find all
	 * associated conceptcds and patient sets.
	 * Will merge all patient sets into a hashset to create a distinct collection of patient nums whom's
	 * size will result in the concept counts
	 * 
	 * @param ccCounts
	 * @param factmap
	 * @throws IOException
	 * @throws CsvDataTypeMismatchException
	 * @throws CsvRequiredFieldEmptyException
	 */
	private static void generateCounts() throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		Map<String, Set<String>> nodeAndConceptCds = new HashMap<String, Set<String>>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){
			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			
			List<ConceptDimension> concepts = csvToBean.parse();
			
			Set<String> conceptNodes = new HashSet<String>();
			Map<String, String> conceptCdSetMap = null;

			conceptCdSetMap = concepts.stream().collect(
						Collectors.toMap(ConceptDimension::getConceptPath, ConceptDimension::getConceptCd)
					);
			
			conceptCdSetMap.keySet().forEach(concept ->{
				String currentPath = concept;
				conceptNodes.add(currentPath);

				int x = StringUtils.countMatches(currentPath, PATH_SEPARATOR);
				
				while(x > 2) {
					
					currentPath = concept.substring(0, StringUtils.ordinalIndexOf(concept, PATH_SEPARATOR, (x - 1)) + 1);
					
					conceptNodes.add(currentPath);
					
					x = StringUtils.countMatches(currentPath, PATH_SEPARATOR);
				}
			});
			
			for(String node: conceptNodes) {
				
				Set<String> conceptCds = new HashSet<String>();
				
				for(String conceptPath : conceptCdSetMap.keySet()) {
				
					if(conceptPath.contains(node)) {
					
						conceptCds.add(conceptCdSetMap.get(conceptPath));
					
					}
					
				};
				nodeAndConceptCds.put(node, conceptCds);

			}
			// ConceptPath and Patient Set
			
		}		
		if( nodeAndConceptCds != null ) compileNodes(nodeAndConceptCds);
 		
	}

	/**
	 * Methods takes concept nodes and generates an array list of concept counts.
	 * 
	 * @param conceptNodes
	 * @param conceptCdSetMap
	 * @param factmap
	 * @return
	 */
	private static void compileNodes(Map<String, Set<String>> nodeAndConceptCds) {
		
		nodeAndConceptCds.entrySet().forEach(entry ->{
			Set<String> patients = new HashSet<String>();
			
			for(String conceptCdFileName: entry.getValue()) {
				try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(PROCESSING_FOLDER + File.separatorChar + conceptCdFileName))){
					Set<String> patientNums = (Set<String>) ois.readObject();
					patients.addAll(patientNums);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				};
			}
			
			try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptCounts.csv"),StandardOpenOption.APPEND, StandardOpenOption.CREATE)){
				ConceptCounts cc = new ConceptCounts();
				cc.setConceptPath(entry.getKey());
				int x = StringUtils.countMatches(entry.getKey(), PATH_SEPARATOR);
				cc.setParentConceptPath(entry.getKey().substring(0, StringUtils.ordinalIndexOf(entry.getKey(), PATH_SEPARATOR, (x - 1)) + 1));
				cc.setPatientCount(entry.getValue().size());
				
				Utils.writeToCsv(buffer, Arrays.asList(cc), DATA_QUOTED_STRING, DATA_SEPARATOR);
				buffer.flush();
				buffer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CsvDataTypeMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CsvRequiredFieldEmptyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 				
		});
	}

	/**
	 * Method loads a map of concept codes with a set of associated patient nums.
	 * This will be used as a lookup when generating counts.
	 * 
	 * @param factmap
	 * @return 
	 * @throws IOException
	 */
	private static int x = 0;
	private static void readFactFile() throws IOException {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ObservationFact.csv"))){
			System.out.println("building csvtobean");
			CsvToBean<ObservationFact> csvToBean = Utils.readCsvToBean(ObservationFact.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			System.out.println("building processing streams");
			csvToBean.parse().stream().forEach(fact -> {
				System.out.println(x);
				x++;
				if(fact.getConceptCd().equalsIgnoreCase("security")) return;
				
				Set<String> patientNums = new HashSet<String>();
				
				if(Files.exists(Paths.get(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))){
					boolean patientExistsAlready = true;
					try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))){
						patientNums = (Set<String>) ois.readObject();
						if(patientNums.contains(fact.getPatientNum())) patientExistsAlready = true;
						else {
							patientNums.add(fact.getPatientNum());
							patientExistsAlready = false;
						}
						ois.close();
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(!patientExistsAlready){
						try(ObjectOutputStream oos = new ObjectOutputStream(
								new FileOutputStream(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))
								){
							oos.writeObject((Set<String>) patientNums);
							oos.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} else {
					try(ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))
							){
						patientNums.add(fact.getPatientNum());
						oos.writeObject((Set<String>) patientNums);
						oos.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
							
			});/*
			csvToBean.forEach(fact -> {
				System.out.println(x);
				x++;
				if(fact.getConceptCd().equalsIgnoreCase("security")) return;
				
				Set<String> patientNums = new HashSet<String>();
				
				if(Files.exists(Paths.get(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))){
					boolean patientExistsAlready = true;
					try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))){
						patientNums = (Set<String>) ois.readObject();
						if(patientNums.contains(fact.getPatientNum())) patientExistsAlready = true;
						else {
							patientNums.add(fact.getPatientNum());
							patientExistsAlready = false;
						}
						ois.close();
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(!patientExistsAlready){
						try(ObjectOutputStream oos = new ObjectOutputStream(
								new FileOutputStream(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))
								){
							oos.writeObject((Set<String>) patientNums);
							oos.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} else {
					try(ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(PROCESSING_FOLDER + File.separatorChar + fact.getConceptCd()))
							){
						patientNums.add(fact.getPatientNum());
						oos.writeObject((Set<String>) patientNums);
						oos.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			});*/
		}
	}
}
