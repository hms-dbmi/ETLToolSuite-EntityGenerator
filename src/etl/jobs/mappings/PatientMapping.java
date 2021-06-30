package etl.jobs.mappings;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.opencsv.CSVReader;

/**
 * Parent class for patient mapping
 * projects should use this generic method 
 * if projects need specific methodology outside of the parent
 * they should cast it to a class the extends this parent.
 *
 */
public class PatientMapping {

	private String sourceId;
	
	private String sourceName;
	
	private Integer patientNum;

	public PatientMapping(String[] pmRecord) {
		super();
		this.sourceId = pmRecord[0];
		this.sourceName = pmRecord[1];
		this.patientNum = new Integer(pmRecord[2]);
	}

	public PatientMapping() {
		// TODO Auto-generated constructor stub
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public Integer getPatientNum() {
		return patientNum;
	}

	public void setPatientNum(Integer patientNum) {
		this.patientNum = patientNum;
	}

	public static List<PatientMapping> readPatientMappingFile(String patientMappingFileURI) {
		
		List<PatientMapping> pm = new ArrayList<PatientMapping>();
		
		if(Files.exists(Paths.get(patientMappingFileURI))) {
			
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get(patientMappingFileURI))) {
				
				CSVReader reader = new CSVReader(buffer);
				
				String[] line; 
				
				while((line = reader.readNext()) != null) {
					
					if(line[0].equals("SOURCE_ID")) continue; // skip header
					
					pm.add(new PatientMapping(line));
					
				}
				
				
			} catch (IOException e) {
				System.err.println("Error reading Patient Mapping File " + patientMappingFileURI);
				e.printStackTrace();
			}
			
		}
		
		return pm;
	}

	public static Map<String, Integer> buildSeqMap(List<PatientMapping> patientMappings) {
		HashMap<String,Integer> map = new HashMap<>();
		
		for(PatientMapping pm : patientMappings) {
			map.put(pm.getSourceId(), pm.getPatientNum());
		}
		return map;
	}

	public static void writePatientMappings(List<PatientMapping> patientMappings, Path path) {
		
		try(BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)){
			
			writer.write(csvHeaders());
			for(PatientMapping pm: patientMappings) {
				writer.write(pm.toCSV());
			}
		} catch (IOException e) {
			System.err.println("Error writing " + path.getFileName());
			e.printStackTrace();
		}
	}

	private static char[] csvHeaders() {
		StringBuilder sb = new StringBuilder();
		sb.append('"');
		sb.append("SOURCE_ID");
		sb.append('"');
		sb.append(',');
		sb.append('"');
		sb.append("SOURCE_NAME");
		sb.append('"');
		sb.append(',');
		sb.append('"');
		sb.append("PATIENT_NUM");
		sb.append('"');
		sb.append('\n');
		return sb.toString().toCharArray();
	}
	public char[] toCSV() {
		StringBuilder sb = new StringBuilder();

		sb.append('"');
		sb.append(this.sourceId);
		sb.append('"');
		sb.append(',');
		sb.append('"');
		sb.append(this.sourceName);
		sb.append('"');
		sb.append(',');
		sb.append('"');
		sb.append(this.patientNum);
		sb.append('"');
		sb.append('\n');
		return sb.toString().toCharArray();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sourceId == null) ? 0 : sourceId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PatientMapping other = (PatientMapping) obj;
		if (sourceId == null) {
			if (other.sourceId != null)
				return false;
		} else if (!sourceId.equals(other.sourceId))
			return false;

		return true;
	}

	public static Set<Integer> getHpdsPatIdKeySet(List<PatientMapping> patientMappings) {
		Set<Integer> pmSet = new TreeSet<Integer>();
		
		for(PatientMapping pm: patientMappings) {
			pmSet.add(pm.getPatientNum());
		}
		
		return pmSet;
	}

	@Override
	public String toString() {
		return "PatientMapping [sourceId=" + sourceId + ", sourceName=" + sourceName + ", patientNum=" + patientNum
				+ "]";
	}
	
	
	
}
