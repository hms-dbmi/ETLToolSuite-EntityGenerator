package etl.data.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONDataSource2 {
	
	private static ObjectMapper objectMapper = new ObjectMapper();	

	public JSONDataSource2() {
	}

	public Object processData(String... arguments) throws FileNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static Class createClassType(String type) throws ClassNotFoundException{
		return Class.forName(type);
	}
	
	private static <T> List<T> createListOfType(Class<T> type){
	    return new ArrayList<T>();
	}
	
	private static <T> TypeReference<List<T>> createTypeReference(Class<T> type){
		
		return new TypeReference<List<T>>(){};
		
	}
	
	public static <T> List buildObjectMap(Object obj, Class<T> _class) throws JsonParseException, JsonMappingException, ClassNotFoundException, IOException {
		if(obj instanceof File){

			//System.out.println(objectMapper.readValue((File) obj, Class.forName(_class)));
			return (List) objectMapper.readValue((File) obj, new TypeReference<List<T>>(){});//createTypeReference(c));
		
		} 
		return null;

	}

	private HashMap<String,Object> processTree(Object obj) throws JsonProcessingException, IOException{
		
		if(obj instanceof File){
		
			return objectMapper.readValue((File) obj, HashMap.class);
		
		} else if ( obj instanceof String){
			
			//JsonNode node = objectMapper.readTree((String) obj);	
						
			return objectMapper.readValue((String) obj, HashMap.class);			
		
		}
		
		return null;
	}
}
