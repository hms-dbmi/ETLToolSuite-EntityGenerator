package etl.data.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;

import au.com.bytecode.opencsv.CSVReader;

public class CSVDataSource2 extends DataSource {
	public static char DELIMITER = ',';
	public static char QUOTE_CHAR = '"';
	public static char ESCAPE_CHAR = '\0';
	public static int SKIP_HEADER = 0;
	public CSVDataSource2(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object processData(String... arguments) throws FileNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public static <T> List buildObjectMap(Object obj, Class<T> _class) throws JsonParseException, JsonMappingException, ClassNotFoundException, IOException {
		if(obj instanceof File){
			
			au.com.bytecode.opencsv.CSVReader reader = new CSVReader(new FileReader((File) obj), DELIMITER, QUOTE_CHAR,ESCAPE_CHAR, SKIP_HEADER);
			
			return buildObjectMapper(reader);
			//System.out.println(objectMapper.readValue((File) obj, Class.forName(_class)));
			
			//return (List) objectMapper.readValue((File) obj, new TypeReference<List<T>>(){});//createTypeReference(c));
		
		} else if ( obj instanceof String){
			
			//JsonNode node = objectMapper.readTree((String) obj);	
						
			//return (List) objectMapper.readValue((String) obj, Class.forName(_class));			
		
		}
		return null;
		//throw new Exception("Invalid Input type for JSON Object");

	}
	
	public static <T> List buildObjectMap(String string, Object obj, Class<T> dATASOURCE_FORMAT) throws IOException {
		if(obj instanceof File){
			
			au.com.bytecode.opencsv.CSVReader reader = new CSVReader(new FileReader((File) obj), DELIMITER, QUOTE_CHAR,ESCAPE_CHAR, 1);
			
			return buildObjectMapper(string, reader);
			//System.out.println(objectMapper.readValue((File) obj, Class.forName(_class)));
			
			//return (List) objectMapper.readValue((File) obj, new TypeReference<List<T>>(){});//createTypeReference(c));
		
		} else if ( obj instanceof String){
			
			//JsonNode node = objectMapper.readTree((String) obj);	
						
			//return (List) objectMapper.readValue((String) obj, Class.forName(_class));			
		
		}
		return null;
		//throw new Exception("Invalid Input type for JSON Object");
	}
	@SuppressWarnings("rawtypes")
	private static List buildObjectMapper(CSVReader reader) throws IOException {
		List list = new ArrayList<>();
		
		for(Object node: reader.readAll()) {
			
			if(node instanceof String[]) {
				LinkedHashMap record = new LinkedHashMap<>();
				String[] line = (String[]) node;
				Integer column = 0;
				for(String variable: line) {
					
					record.put(column.toString(),Arrays.asList(variable));
					
					column++;
					
				}
				list.add(record);

			}
			
			
		}
		return list;
	}
	@SuppressWarnings("rawtypes")
	private static List buildObjectMapper(String keyPrefix, CSVReader reader) throws IOException {
		List list = new ArrayList<>();
		
		for(Object node: reader.readAll()) {
			
			if(node instanceof String[]) {
				LinkedHashMap record = new LinkedHashMap<>();
				String[] line = (String[]) node;
				Integer column = 0;
				for(String variable: line) {
					
					record.put(keyPrefix + ":" + column.toString(),Arrays.asList(variable));
					
					column++;
					
				}
				list.add(record);

			}
			
			
		}
		return list;
	}

	public au.com.bytecode.opencsv.CSVReader processCSV(String csvFile, char delimiter, int skipHeader) throws FileNotFoundException{
		
		if(new File(csvFile).isFile()) {
			
			au.com.bytecode.opencsv.CSVReader csvreader = new CSVReader(new FileReader(csvFile), delimiter, '"','\0', skipHeader);
			
			return csvreader;
		} else {
			return null;
		}
		
	}
	
	public com.opencsv.CSVReader processCSV(String csvFile) throws IOException{
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
		com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(reader);
		return csvReader;
		
	}

}
