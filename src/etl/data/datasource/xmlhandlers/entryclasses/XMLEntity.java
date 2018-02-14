package etl.data.datasource.xmlhandlers.entryclasses;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.jdom2.Element;

public class XMLEntity {

	public void setFields(Class<?> anon, Element elem) throws IllegalArgumentException, IllegalAccessException{
		Field[] fields = anon.getDeclaredFields();
		for(Field field:fields){
			field.set(field, "test");
		}
	}
	
	protected Map<String,String> setAttributesToMap(Element elem){
		Map<String, String> attributes = new HashMap<String, String>();
		
		elem.getAttributes().forEach(attr -> {
			attributes.put(attr.getQualifiedName(), attr.getValue());
		});
		
		return attributes;
	}
}
