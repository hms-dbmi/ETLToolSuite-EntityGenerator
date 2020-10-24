package etl.jobs.csv.bdc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.jobproperties.JobProperties;
import etl.jobs.mappings.PatientMapping;

public class SetPatientSequence extends BDCJob{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8425241941100183446L;

	private static class StudySequencingMeta {
		
		public String studyName = "";
		
		public boolean hasJobConfig = false;
		
		public boolean hasPatientMapping = false;
		
		public boolean hasSubjectMulti = false;
		
		public int startingSeqNum = -1;
		
		public Set<String> patientCount = new HashSet<>();
		
		public StudySequencingMeta() {
			
		}

		@Override
		public String toString() {
			
			return "trialid=" + studyName + "\n" + 
					"patientnumstartseq=" + startingSeqNum  + "\n" +
					"patientmappingfile=" + studyName.toUpperCase() + "_PatientMapping.v2.csv" + "\n" +
					"usepatientmapping=" + "Y" + "\n" +
					"patientcol=" + "0";
		}
		
	}
	
	public static Map<String,StudySequencingMeta> seqMeta =  new HashMap<String,StudySequencingMeta>();
	
	public static void main(String[] args) {
		try {
			
			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}
		
		try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			e.printStackTrace();
		} 
	}

	private static void execute() throws IOException {
		
		List<BDCManagedInput> managedInputs = getManagedInputs();
		
		for(BDCManagedInput managedInput: managedInputs) {
			buildSeqMetaMap(managedInput);
		}
		
		setSequences();
		
		detectCollisions();
		
		printConfigs();
		
		updateMappings();
	}

	private static void updateMappings() {
		// key = study name, value = StudySequencingMeta
		seqMeta.entrySet().stream().forEach(ssmEntry -> {
			
			List<PatientMapping> patientMappings = PatientMapping.readPatientMappingFile(DATA_DIR + ssmEntry.getKey() + "_PatientMapping.v2.csv");
			
			if(patientMappings.isEmpty()) System.err.println(ssmEntry.getKey() + " patient mapping does not exist generating new one.");
			
			Map<String, Integer> seqMap = PatientMapping.buildSeqMap(patientMappings);
			
			Integer maxId = Collections.max(seqMap.values()) + 1;
			
			for(String patient: ssmEntry.getValue().patientCount) {
				if(seqMap.containsKey(patient)) continue;
				else {
					
					String[] p = new String[] { patient, ssmEntry.getKey(), maxId.toString()};
					
					PatientMapping pm = new PatientMapping(p);
					
					patientMappings.add(pm);
					
					maxId++;
				}
			}
			
			PatientMapping.writePatientMappings(patientMappings,Paths.get(WRITE_DIR + ssmEntry.getKey() + "_PatientMapping.v2.csv"));
		});
		
	}

	private static void detectCollisions() {
		for(Entry<String, StudySequencingMeta> entry: seqMeta.entrySet()) {
			Range<Integer> sequenceRange = Range.between(entry.getValue().startingSeqNum, entry.getValue().startingSeqNum + entry.getValue().patientCount.size());
			
			detectCollisions(entry.getKey(),sequenceRange);
		}
	}

	private static void detectCollisions(String studyName, Range<Integer> sequenceRange) {
		
		for(Entry<String, StudySequencingMeta> entry: seqMeta.entrySet()) {
			
			if(entry.getKey().equalsIgnoreCase(studyName)) continue;
			
			if(sequenceRange.contains(entry.getValue().startingSeqNum)) {
				System.err.println("Patient Collision between " + studyName + " and " + entry.getKey());
			}
			
		}
		
	}

	private static void printConfigs() throws IOException {
		for(Entry<String, StudySequencingMeta> entry: seqMeta.entrySet()) {
			try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + entry.getValue().studyName.toUpperCase() + "_job.config"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				buffer.write(entry.getValue().toString());
			}
		}
	}

	private static void setSequences() {
		
		int maxSeq = getMaxSeq();
		
		for(Entry<String, StudySequencingMeta> entry: seqMeta.entrySet()) {
			StudySequencingMeta ssm = entry.getValue();
			
			if(ssm.startingSeqNum == -1) {
				ssm.startingSeqNum = maxSeq;
				maxSeq += ssm.patientCount.size() + 1000;
			}
			
		}
		
	}

	private static int getMaxSeq() {
		int max = 1;
		for(Entry<String, StudySequencingMeta> entry: seqMeta.entrySet()) {
			if(entry.getValue().startingSeqNum > max) max = entry.getValue().startingSeqNum + entry.getValue().patientCount.size() + 100;
		}
		return max;
	}

	private static void buildSeqMetaMap(BDCManagedInput managedInput) throws IOException {
				
		StudySequencingMeta ssm = new StudySequencingMeta();
		
		ssm.studyName = managedInput.getStudyAbvName();
		
		ssm.hasJobConfig = hasJobConfig(managedInput);
		
		if(ssm.hasJobConfig) {
			ssm.startingSeqNum = getStartingSeqNum(managedInput);
		}
		
		ssm.hasPatientMapping = hasPatientMapping(managedInput);
		
		ssm.patientCount = getPatientSet(managedInput);
		
		seqMeta.put(managedInput.getStudyAbvName(), ssm);
		
	}
	
	private static Set<String> getPatientSet(BDCManagedInput managedInput) throws IOException {
		
		// if missing subject multi file fail job completely as it is critical to have all patient counts for new data load
		if(BDCJob.getStudySubjectMultiFile(managedInput) == null) {
			throw new IOException("Critical error: " + managedInput.toString() + 
					" missing subject multi file! All studies must have a subject multi file to proceed ahead!");
		}
		
		Set<String> patientSet = new HashSet<>();
		
		try(CSVReader reader = readRawBDCDataset(Paths.get(DATA_DIR + BDCJob.getStudySubjectMultiFile(managedInput)),true)){
			reader.readNext(); // skip header
			String[] line;
			
			if(seqMeta.containsKey(managedInput.getStudyAbvName())) {
				patientSet = seqMeta.get(managedInput.getStudyAbvName()).patientCount;
			} 
			while((line = reader.readNext()) != null) {
				patientSet.add(line[0]);
			}
			
		}
		return patientSet;
	}

	private static int getStartingSeqNum(BDCManagedInput managedInput) {
		String jobConfig = RESOURCE_DIR + managedInput.getStudyAbvName().toUpperCase() + "_job.config";
		
		try {
			JobProperties prop = new JobProperties().buildProperties(jobConfig);
			
			if(prop.containsKey("patientnumstartseq")) {
				
				if(NumberUtils.isCreatable(prop.get("patientnumstartseq").toString())) {
					return new Integer(prop.get("patientnumstartseq").toString()); 
				} else {
					throw new Exception(jobConfig + " has invalid patientnumstartseq");
				}
				
			}
			
		} catch (Exception e) {
			System.err.println("Bad config file" + RESOURCE_DIR + managedInput.getStudyAbvName() + "_job.config");
			e.printStackTrace();
		}
		
		return 0;
	}

	private static boolean hasPatientMapping(BDCManagedInput managedInput) throws IOException {
		File file = new File(DATA_DIR);

		if(file.isDirectory()) {
			
			String[] files = file.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					
					if(name.equalsIgnoreCase(managedInput.getStudyAbvName() + "_PatientMapping.v2.csv")) {
						return true;
					}
					
					return false;
					
				}
				
			});
			
			if(files.length > 0) {
				return true;
			} else {
				return false;
			}
			
		} else {
			throw new IOException(DATA_DIR + " is not a directory!");
		}
	}

	private static boolean hasJobConfig(BDCManagedInput managedInput) throws IOException {

		File file = new File(RESOURCE_DIR);

		if(file.isDirectory()) {
			
			String[] files = file.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					
					if(name.equalsIgnoreCase(managedInput.getStudyAbvName() + "_job.config")) {
						return true;
					}
					
					return false;
					
				}
				
			});
			
			if(files.length > 0) {
				return true;
			} else {
				return false;
			}
			
		} else {
			throw new IOException(RESOURCE_DIR + " is not a directory!");
		}
	}
	
}
