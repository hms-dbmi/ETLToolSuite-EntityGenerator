package etl.data.datasource.xmlhandlers.entryclasses;

import java.util.HashMap;
import java.util.Map;

import org.jdom2.Element;

public class Act extends XMLEntity{
	private Map<String,String> act;
	private Map<String,String> templateId;
	private Map<String,String> code;
	private Text text;
	private Map<String,String> statusCode;
	
	public Act(Element elem) {
		super();
		this.act = setAttributesToMap(elem);
	}

	
}
