package etl.job.jobtype;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.jdom2.Content;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonProcessingException;

import etl.data.datasource.XMLDataSource;
import etl.data.datasource.entities.CriteriaNode;import etl.data.datasource.xmlhandlers.entryclasses.XMLEntity;
import etl.job.jobtype.properties.JobProperties;


public class XmlToI2b2TM extends JobType {

	private static final String FILE_NAME = "/Users/tom/Documents/59b03b97e5811e4242359805.xml";
	
	private static final String ROOT_ELEMENT = "structuredBody";
	
	private static final CriteriaNode criteriaNode = new CriteriaNode();
	
	
	static {
		
		criteriaNode.setParent("entry");
		
		List<Object> inclusionNodes = new ArrayList<Object>(){};
		
		inclusionNodes.add("act");
		
		criteriaNode.setInclusionNodes(inclusionNodes);
	
	};

	private Namespace ns = Namespace.getNamespace("urn:hl7-org:v3");
	
	
	public XmlToI2b2TM(String str) throws Exception {
		super(str);
	}

	@Override
	public void runJob(JobProperties jobProperties) {

		try {
			
			buildRecords();
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}



	private List<Map<String, String>> buildRecords() throws Exception{
		
		XMLDataSource xmlDS = new XMLDataSource("XMLFILE");
		
		org.jdom2.Document doc = xmlDS.processXml(FILE_NAME);
		
		Element root = doc.getRootElement();		
		
		List<CriteriaNode> critNodes = new ArrayList<CriteriaNode>();
		
		critNodes.add(criteriaNode);
		
		List<Element> elems = getAllChildrenForElementWithCriteria(root, critNodes);

		//List<Element> elems = getAllChildren(root);
		
		Set<String> parents = CriteriaNode.class.newInstance().getParents(critNodes);
		
		elems.forEach(entry -> {
			
			buildObjects(entry);
			
		});
		
		/*
		root = root.getChild("component",ns);
		
		System.out.println(root);
		
		root = root.getChild("structuredBody", ns);
		
		// root = root.getChild("component", ns);
		
		//root = root.getChild("section", ns);
		
		System.out.println(root);
		
		List<Element> elems = root.getChildren("component", ns);
		
		elems.forEach(elem -> {
			System.out.println(elem.getChild("section", ns));
		});
		*/
		return null;
	}
		
	private void buildObjects(Element element){
		List<Element> elems = element.getChildren();
		
		elems.forEach(elem -> {

			Class<?> someClass = null;
			try {
				someClass = Class.forName("etl.data.datasource.xmlhandlers.entryclasses." + StringUtils.capitalize(elem.getQualifiedName()));
			} catch (Exception e) {
				
				try{
					
					someClass = Class.forName("etl.data.datasource.xmlhandlers.entryclasses." + StringUtils.capitalize(elem.getParentElement().getQualifiedName()));
				
				} catch (Exception e2) {
					/*
					System.err.println(e2);
					System.out.println(elem);
					System.err.println(elem.getParentElement().getQualifiedName());
*/
				}
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
			if(someClass != null){
				//System.out.println(someClass);
				XMLEntity xmlentity = new XMLEntity();
				
				try {
					xmlentity.setFields(someClass, elem);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
			
			//newInstance =  (JobType) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(jobType);
			
			//System.out.println(StringUtils.capitalize(elem.getQualifiedName()));
			buildObjects(elem);
			
		});
		
	}
	
	private List<Element> getAllChildren(Element parent){
		List<Element> sibs = new ArrayList<Element>();
		
		addAllChildren(parent, sibs);
		
		//System.out.println("sibs: " + sibs);
		
		return sibs;	
	}
	
	private List<Element> getAllChildrenForElementWithCriteria(Element parent, List<CriteriaNode> criteriaNodes){
		List<Element> sibs = new ArrayList<Element>();
		
		addAllChildren(parent, sibs, criteriaNodes);
		
		//sibs.forEach(System.out::println);
		
	
		return sibs;	
	}
	
	private List<Element> addAllChildren(Element parent, List<Element> sibs){
		
		List<Element> n1 = parent.getChildren();
		
		for (int i = 0; i < n1.size(); i++) {
		       Element e = n1.get(i);
		       
		       if (e instanceof Element){
		   		   
		    	   sibs.add((Element) e);
		
		    	   addAllChildren(e, sibs); 
		       }
		    
		}	
		
		return sibs;
		
	}
	
	private List<Element> addAllChildren(Element parent, List<Element> sibs, List<CriteriaNode> criteriaNodes){
		
		List<Element> n1 = parent.getChildren();

		for (int i = 0; i < n1.size(); i++) {
			Element e = n1.get(i);
			
			CriteriaNode hasCrit = criteriaNodes.stream()
				.filter(crit -> crit.getParent().toString().equalsIgnoreCase(e.getQualifiedName()))
				.findAny()
				.orElse(null);
			
			if(hasCrit != null){
				
				sibs.add((Element) e);

			}
			
			addAllChildren(e, sibs, criteriaNodes); 
		
		}
    
			
		return sibs;

	}

	@Override
	public void setVariables(JobProperties jobProperties) {
		// TODO Auto-generated method stub
		
	}
}