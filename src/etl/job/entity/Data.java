package etl.job.entity;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.opencsv.CSVReader;

import etl.jobs.mappings.Mapping;

public class Data {
	protected String fileName;
	protected String dataLabel;
	protected String dataType;
	protected int colIndex;

	private List<DataRecord> dataRecords;
	
	public Data() {
		// TODO Auto-generated constructor stub
	}

	public Data(String fileName, String dataLabel, String dataType, int colIndex, List<DataRecord> dataRecords) {
		super();
		this.fileName = fileName;
		this.dataLabel = dataLabel;
		this.dataType = dataType;
		this.colIndex = colIndex;
		this.dataRecords = dataRecords;
	}

	public static List<Data> buildDataList(String dataURI, char dataFileDelimiter, char dataFileQuotedString,
			boolean dataFileAnalyze, int analyzeThreshold) {

		File file = new File(dataURI);
		
		if(file.exists()) {
			if(dataFileAnalyze) {
				// perform analysis
			} 
			
		}
		
		return null;
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
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

	public int getColIndex() {
		return colIndex;
	}

	public void setColIndex(int colIndex) {
		this.colIndex = colIndex;
	}

	public List<DataRecord> getDataRecords() {
		return dataRecords;
	}

	public void setDataRecords(List<DataRecord> dataRecords) {
		this.dataRecords = dataRecords;
	}

	/**
	 * 
	 *  Used to build mapping file
	 *  This is much more lightweight than the full datafile processing used in analzye;
	 * @param dataURI
	 * @param dataFileHasHeaders
	 * @param dataFileDelimiter
	 * @param dataFileQuotedString
	 * @return
	 * @throws Exception
	 */
	public static List<Data> buildDataListHeaders(String dataURI, boolean dataFileHasHeaders,
			char dataFileDelimiter, char dataFileQuotedString) throws Exception {
		List<Data> dataList = new ArrayList<Data>();
		
		if(dataFileHasHeaders) {
			
			File file = new File(dataURI);
			
			if(file.exists()) {
			
				if(file.isDirectory()) {
				
				} else {
					
					String fileName = file.getName();
					
					CSVReader csvReader = new CSVReader(new BufferedReader(new FileReader(file)), dataFileDelimiter, dataFileQuotedString);
					if(dataFileHasHeaders) {
						
						String[] headers = csvReader.readNext();
						int colNum = 0;
						
						for(String header:headers) {
							
							Data data = new Data(fileName, header, "", colNum, new ArrayList<DataRecord>());
							dataList.add(data);
							colNum++;
							
						}
						
					} else {
						// just use Column index as data Label
						String[] firstRow = csvReader.readNext();
						int colNum = 0;
						
						for(String row:firstRow) {
							
							Data data = new Data(fileName, "COLUMN " + colNum, "", colNum, new ArrayList<DataRecord>());
							dataList.add(data);
							colNum++;
							
						}
					}
				}
			} else {
				
				throw new Exception("Data file/dir " + dataURI + " does not exist or is unreachable!");
			
			}
			
		} 

		return dataList;
		
	}
	
	public static List<Data> buildDataList(String dataURI, boolean dataFileHasHeaders,
			char dataFileDelimiter, char dataFileQuotedString) throws Exception {
		List<Data> dataList = new ArrayList<Data>();
		
		if(dataFileHasHeaders) {
			
			File file = new File(dataURI);
			
			if(file.exists()) {
				String fileName = file.getName();
				CSVReader csvReader = new CSVReader(new BufferedReader(new FileReader(file)), dataFileDelimiter, dataFileQuotedString);
				
				String[] headers;
				
				if(dataFileHasHeaders) {
					
					headers = csvReader.readNext();
					
				} else {
					String[] firstRow = csvReader.readNext();
					int colNum = 0;
					List<String> l = new ArrayList<String>();
					for(String row:firstRow) {
						
						l.add(colNum, row);
						colNum++;
						
					}
					headers = l.toArray(new String[0]);
				}
				csvReader.close();
				
				csvReader = new CSVReader(new BufferedReader(new FileReader(file)), dataFileDelimiter, dataFileQuotedString);
				int rowIndex = 0;
				Iterator<String[]> iter = csvReader.iterator();
				while(iter.hasNext()) {
					
					String[] rec = iter.next();
					
					int colIndex = 0;
					
					for(String cell: rec) {
						
						Data d = null;
						if(dataList.isEmpty() || dataList.size() == colIndex) {
							dataList.add(new Data(fileName, headers[colIndex], "", colIndex, new ArrayList<DataRecord>()));								
						} else if(dataList.size() > colIndex){
							dataList.get(colIndex).getDataRecords().add(new DataRecord(rowIndex, colIndex, cell));							
						} else if(dataList.size() - 1 == colIndex) {
							dataList.get(colIndex).getDataRecords().add(new DataRecord(rowIndex, colIndex, cell));
						}

						colIndex++;
					}
					
					rowIndex++;
				}
				
			} else {
				
				throw new Exception("Data file/dir " + dataURI + " does not exist or is unreachable!");
			
			}
			
		} 

		return dataList;
		
	}
	public static List<Mapping> generateMappingList(String dataURI, List<Data> dataList) throws Exception {
		File file = new File(dataURI);
		List<Mapping> mappings = new ArrayList<Mapping>();
		
		if(file.exists()) {
			if(file.isDirectory()) {
				for(Data data:dataList) {
					for(File f: file.listFiles()) {
						
						String fileName = f.getName();
						
						String key = fileName + ":" + data.colIndex;
						String rootNode = "\\" + data.dataLabel + "\\";
						String supPath = "";
						String dataType = "";
						String options = "";
						
						Mapping mapping = new Mapping(key, rootNode, supPath, dataType, options);
	
						mappings.add(mapping);
					}
				}
			} else {
				for(Data data:dataList) {
					
					String fileName = file.getName();
					
					String key = fileName + ":" + data.colIndex;
					String rootNode = "\\" + data.dataLabel + "\\";
					String supPath = "";
					String dataType = "";
					String options = "";
					
					Mapping mapping = new Mapping(key, rootNode, supPath, dataType, options);

					mappings.add(mapping);
				}
			}
		} else {
			
			throw new Exception("Data file " + dataURI + " does not exist or is unreachable!");
		}
		
		mappings.add(0,new Mapping("key","Concept Path","Supplement Path","Data Type","Options"));
		return mappings;
	}

	public static List<Mapping> generateMappingList(List<Data> fullData) {
		List<Mapping> mappings = new ArrayList<Mapping>();
		
		for(Data d: fullData) {
			mappings.add(new Mapping(d.getFileName() + ":" + d.getColIndex(), "\\" + d.getDataLabel() + "\\", "", d.getDataType(), ""));
			
		}
		mappings.add(0,new Mapping("key","Concept Path","Supplement Path","Data Type","Options"));

		return mappings;
	}

	
}
