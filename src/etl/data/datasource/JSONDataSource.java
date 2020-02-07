package etl.data.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;



//  This class invokes Jackson to parse JSON files.
//  Typically it will just convert the JSON in to JAVA Objects.
public class JSONDataSource {
	Map m2 = new HashMap<String, List<String>>();

	public JSONDataSource() throws Exception {
	}

	private ObjectMapper objectMapper = new ObjectMapper();	
	
	public JsonNode buildObjectMap(Object obj) throws JsonProcessingException, IOException{
		
		JsonNode node1 = processTree(obj);

		return node1;

	}
	
	public Map<String,String> processNodeToMap(JsonNode node) throws JsonProcessingException, IOException{

		Map<String,String> map = new HashMap<String,String>();
		
		String currKey = "";
		
		if(node == null){

			return null;
		
		}
		
		if(node.isArray()){
		
			map.put(currKey, node.toString());
			
			for(int x = 0 ; node.size() > x ; x++){
				
				processNodeToMap(node.get(x), map, currKey);
			
			}
		
		}
		Iterator<Entry<String,JsonNode>> iter = node.fields();
		
		while(iter.hasNext()){
			
			Entry<String,JsonNode> n = iter.next();

			if(!n.getValue().isNull() ) map.put(currKey + ":" + n.getKey(), n.getValue().asText());
			
			processNodeToMap(n.getValue(), map, currKey + ":" + n.getKey());
			
		}
			
		return map;
	}
	
	private JsonNode processTree(Object obj) throws JsonProcessingException, IOException{
		
		if(obj instanceof File){
		
			return objectMapper.readTree((File) obj);
		
		} else if ( obj instanceof String){
			
			//JsonNode node = objectMapper.readTree((String) obj);	
						
			return objectMapper.readTree((String) obj);			
		
		}
		
		return null;
	}
	
	private Map<String,String> processNodeToMap(JsonNode node, Map<String,String> map, String currKey) throws JsonProcessingException, IOException{
		if(node == null){
			return null;
		}
		
		if(node.isArray()){
			map.put(currKey, node.toString());
			
			for(int x = 0 ; node.size() > x ; x++){
				processNodeToMap(node.get(x), map, currKey + x);
			
			}
		
		}
		
		Iterator<Entry<String,JsonNode>> iter = node.fields();
		
		if(node.isObject()){
			map.put(currKey,node.toString());
		
		}
		
		while(iter.hasNext()){
			
			Entry<String,JsonNode> n = iter.next();
			
			if(!n.getValue().isNull()) {	
				
				String val = n.getValue().toString();
				
				map.put(currKey + ":" + n.getKey(), n.getValue().asText());
			
			} 
			
			processNodeToMap(n.getValue(),map, currKey + ":" + n.getKey());
		}
		
		return map;
	}	
	
	public List<Map<String, List<String>>> processJsonArrayToMap(String str){
		
		List<Map<String, List<String>>> list = new ArrayList<Map<String,List<String>>>();
		
		try {

			JsonNode node = objectMapper.readTree(str);
			
			if(node.isArray()){
				
				Iterator<JsonNode> iter = node.iterator();
				
				while(iter.hasNext()) {
					
					JsonNode nextNode = iter.next();
					
					Map<String, List<String>> keyVals = new HashMap<String, List<String>>();
					
					traverseTree(nextNode, keyVals);
					
					list.add(keyVals);
					
				}
			
				//traverseTree(node, list, "");
			
			} else if(node.isObject()) {  
				
				Map<String, List<String>> keyVals = new HashMap<String, List<String>>();
				
				traverseTree(node, keyVals);
				
				list.add(keyVals);
				
			} else {
				//System.err.println(node + " is not an array");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
		
	}
	
	private void traverseTree(JsonNode node, Map<String, List<String>> keyVals) {
		
		if(node.isObject()){
			Iterator<String> fields = node.fieldNames();
			
			fields.forEachRemaining(field -> {
				
				if(node.get(field).isValueNode()){

					traverseTreeValueNode(field, node.get(field), keyVals);

				} else if (node.get(field).isArray()){
					
					traverseTreeArrayNode(field, node.get(field), keyVals);

				} else {
					
					traverseTree(node.get(field),keyVals);
					
				}
				
			});
			
		}
	}

	private void traverseTreeArrayNode(String field, JsonNode node,
			Map<String, List<String>> keyVals) {
		
		Iterator<JsonNode> iter = node.iterator();
		
		iter.forEachRemaining(innerNode -> {
			
			if(innerNode.isObject()){
				
				traverseTree(innerNode, keyVals);
			
			} else if(innerNode.isValueNode()){
				
				traverseTreeValueNode(field, innerNode, keyVals);
				
			} else if(innerNode.isArray()){
				
				traverseTreeArrayNode(field, innerNode, keyVals);
			
			}
		});
		
	}

	private void traverseTreeValueNode(String field, JsonNode node, Map<String, List<String>> keyVals){
		
		List<String> strings = keyVals.containsKey(field) ? (ArrayList<String>) keyVals.get(field): new ArrayList<String>();
		strings.add(node.asText());
		
		if(!node.isNull()) keyVals.put(field, strings);		
	}
	
	@Deprecated
	private List<Map<String, List<String>>> traverseTree(JsonNode node, List<Map<String, List<String>>> list){
		// if node is array iterate over it generating a new map of value=strings add that to the list of values
		if(node.isArray()){
			
			Iterator iter = node.iterator();
			
			while(iter.hasNext()){
				
				JsonNode jn = (JsonNode) iter.next();
				if(jn.isValueNode()) {
					
					
				} 
		
			}
			
		}
		
		return list;
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	private List<Map<String, List<String>>> traverseTree(JsonNode node, List<Map<String, List<String>>> list, String _field){

		if(node.isArray()){
			
			Iterator iter = node.iterator();
			
			while(iter.hasNext()){
			
				JsonNode jn = (JsonNode) iter.next();
				
				if(jn.isValueNode()) {
					
					List<String> strings = m2.containsKey(_field) ? (ArrayList<String>) m2.get(_field): new ArrayList<String>();
					
					strings.add(jn.asText());
					
					if(!jn.isNull()) m2.put(_field, strings);
					
				} else {
					
					traverseTree(jn, list, _field);
				
				}
			}
			
		} else if(node.isObject()){

			Iterator<String> fields = node.fieldNames();
			
			fields.forEachRemaining(field -> {

				if(node.get(field).isValueNode()){
					
					List<String> strings = m2.containsKey(field) ? (ArrayList<String>) m2.get(field): new ArrayList<String>();
					
					strings.add(node.get(field).asText());
					
					if(!node.get(field).isNull()) m2.put(field, strings);


				} else {
					
					traverseTree(node.get(field),list, field);
					
				}
				
			});			
		} 
		
		if(m2.size() > 0) list.add(m2);

		return list;
	}
	
}
