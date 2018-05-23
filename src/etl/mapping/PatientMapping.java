package etl.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import etl.data.export.entities.i2b2.PatientDimension;

public class PatientMapping extends etl.mapping.Mapping{
	private String fileName;
	private String patId;
	private String dobCol;
	private String dodCol;
	private String ageCol;
	private String sexCol;
	private String raceCol;
	
	public PatientMapping(){
		super();
	}
	
	public PatientMapping(String[] arr) {
		super();
		this.fileName = arr[0];
		this.patId = arr[1];
		this.dobCol = arr[2];
		this.dodCol = arr[3];
		this.ageCol = arr[4];
		this.sexCol = arr[5];
		this.raceCol = arr[6];
	}
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getPatId() {
		return patId;
	}
	public void setPatId(String patId) {
		this.patId = patId;
	}
	public String getDobCol() {
		return dobCol;
	}
	public void setDobCol(String dobCol) {
		this.dobCol = dobCol;
	}
	public String getDodCol() {
		return dodCol;
	}
	public void setDodCol(String dodCol) {
		this.dodCol = dodCol;
	}
	public String getAgeCol() {
		return ageCol;
	}
	public void setAgeCol(String ageCol) {
		this.ageCol = ageCol;
	}
	public String getSexCol() {
		return sexCol;
	}
	public void setSexCol(String sexCol) {
		this.sexCol = sexCol;
	}
	public String getRaceCol() {
		return raceCol;
	}
	public void setRaceCol(String raceCol) {
		this.raceCol = raceCol;
	}

	public static Map<String, List<PatientMapping>> processMapping(
			CSVReader mappingReader) throws IOException {
		Map<String,List<PatientMapping>> map = new HashMap<String,List<PatientMapping>>();

		for(String[] arr: mappingReader.readAll()){
			
			if(map.containsKey(arr[0])){
				
				List<PatientMapping> list = map.get(arr[0]);

				list.add(new PatientMapping(arr));
				
				map.put(arr[0], list);
				
			} else {
				
				List<PatientMapping> list = new ArrayList<PatientMapping>();

				list.add(new PatientMapping(arr));
				
				map.put(arr[0], list);
			}
			
		}
			

		
		return map;
	}
	public static List<PatientDimension> processMapping(LinkedHashMap<String, Object> m, CSVReader mappingReader) throws IOException{
		
		List<PatientDimension> list = new ArrayList<PatientDimension>();
		
		Map<String, List<PatientMapping>> mappings = processMapping(mappingReader);
		
		for(String key: mappings.keySet()) {
			
		}
		
		return list;
		
	};
	

	@Override
	public void buildMapping(String[] arr) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, List<Mapping>> processMapping(Object... args) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
