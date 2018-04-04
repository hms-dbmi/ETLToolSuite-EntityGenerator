package etl.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;

import com.csvreader.CsvReader;

public class CsvToI2b2TMMapping extends etl.mapping.Mapping{
	private String fileName;
	private String categoryCode;
	private String columnNumber;
	private String dataLabel;
	private String dataType;
	private String patientIdColumn;
	
	private String visitIdColumn;
	private String providerIdColumn;
	
	
	public CsvToI2b2TMMapping(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void buildMapping(String[] arr) throws IOException {

		this.fileName = arr[0];
		this.categoryCode = arr[1];
		this.columnNumber = arr[2];
		this.dataLabel = arr[3];
		this.dataType = arr[4];
		this.patientIdColumn = arr[5];
		this.visitIdColumn = arr[6];
		this.providerIdColumn = arr[7];
	}
	
	@Deprecated
	public void buildCsvToI2b2TMMapping(String[] arr) throws IOException {
		
		this.fileName = arr[0];
		this.categoryCode = arr[1];
		this.columnNumber = arr[2];
		this.dataLabel = arr[3];
		this.dataType = arr[4];
		this.patientIdColumn = arr[5];
		this.visitIdColumn = arr[6];
		this.providerIdColumn = arr[7];
	}
	
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getCategoryCode() {
		return categoryCode;
	}

	public void setCategoryCode(String categoryCode) {
		this.categoryCode = categoryCode;
	}

	public String getColumnNumber() {
		return columnNumber;
	}

	public void setColumnNumber(String columnNumber) {
		this.columnNumber = columnNumber;
	}

	public String getDataLabel() {
		return dataLabel;
	}

	public void setDataLabel(String dataLabel) {
		this.dataLabel = dataLabel;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	public String getPatientIdColumn() {
		return patientIdColumn;
	}


	public void setPatientIdColumn(String patientIdColumn) {
		this.patientIdColumn = patientIdColumn;
	}


	public String getVisitIdColumn() {
		return visitIdColumn;
	}

	public void setVisitIdColumn(String visitIdColumn) {
		this.visitIdColumn = visitIdColumn;
	}

	public String getProviderIdColumn() {
		return providerIdColumn;
	}


	public void setProviderIdColumn(String providerIdColumn) {
		this.providerIdColumn = providerIdColumn;
	}

	@Deprecated
	public List<CsvToI2b2TMMapping> processMapping(CSVReader mappingReader) throws Exception {
		
		List<CsvToI2b2TMMapping> list = new ArrayList<CsvToI2b2TMMapping>();

		try {
			
			for(String[] arr: mappingReader.readAll()){
													
				CsvToI2b2TMMapping mapping = new CsvToI2b2TMMapping("CsvToI2b2TMMapping");
				
				mapping.buildMapping(arr);
				
				list.add(mapping);
				
			}
				
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	/**
	 * CSV Mapping will process a csv file this should be pretty reusalbe through out other jobs
	 * send in a object Cast it to a csv reader and process the mapping file.
	 */
	@Override
	public Map<String, List<Mapping>> processMapping(Object... args) {
		
		Map<String,List<Mapping>> map = new HashMap<String,List<Mapping>>();

		CSVReader mappingReader = ( CSVReader ) args[0];
		
		//Map<String,List<CsvToI2b2TMMapping>> map = new HashMap<String,List<CsvToI2b2TMMapping>>();

		try {
			
			for(String[] arr: mappingReader.readAll()){
				
				if(map.containsKey(arr[0])){
					
					List<Mapping> list = map.get(arr[0]);
					
					this.buildMapping(arr);
					
					list.add(this);
					
					map.put(arr[0], list);
					
				} else {
					
					List<Mapping> list = new ArrayList<Mapping>();

					this.buildMapping(arr);
					
					list.add(this);
					
					map.put(arr[0], list);
				}
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return map;
	}

	public static Set<String> getFileNames(List<CsvToI2b2TMMapping> mappings) {
		
		Set<String> fileNames = new HashSet<String>();
		
		for(CsvToI2b2TMMapping mapping:mappings){
			
			if(!mapping.getFileName().isEmpty()){
				
				fileNames.add(mapping.getFileName());
			
			}
			
		}
		
		return fileNames;
	}

	@Override
	public String toString() {
		return "CsvToI2b2TMMapping [fileName=" + fileName + ", categoryCode="
				+ categoryCode + ", columnNumber=" + columnNumber
				+ ", dataLabel=" + dataLabel + ", dataType=" + dataType
				+ ", patientIdColumn=" + patientIdColumn + ", visitIdColumn="
				+ visitIdColumn + ", providerIdColumn=" + providerIdColumn
				+ "]";
	}
	
}
