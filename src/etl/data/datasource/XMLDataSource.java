package etl.data.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.jdom2.input.DOMBuilder;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


// initial datasource 
public class XMLDataSource extends DataSource {
		
	Map m2 = new HashMap<String, List<String>>();	
	
	
	public XMLDataSource(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}
	
	public org.jdom2.Document processXml(Object obj) throws JsonProcessingException, IOException, ParserConfigurationException, SAXException{
		
		if(obj instanceof File){
			
			return null;
		
		} else if ( obj instanceof String){
			
						
			return useDOMParser(obj.toString());		
		
		}
		
		return null;
	}
	
    private static org.jdom2.Document useDOMParser(String fileName)
            throws ParserConfigurationException, SAXException, IOException {
     
    	//creating DOM Document
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        
        DocumentBuilder dBuilder;
        
        dBuilder = dbFactory.newDocumentBuilder();
        
        Document doc = dBuilder.parse(new File(fileName));
        
        DOMBuilder domBuilder = new DOMBuilder();
        
        return domBuilder.build(doc);
        
    }

	@Override
	public Object processData(String... arguments) throws FileNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}
}
