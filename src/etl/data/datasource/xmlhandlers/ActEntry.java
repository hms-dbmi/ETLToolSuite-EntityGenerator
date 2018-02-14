package etl.data.datasource.xmlhandlers;

import java.util.Map;

import etl.data.datasource.xmlhandlers.entryclasses.EffectiveTime;
import etl.data.datasource.xmlhandlers.entryclasses.Entry;
import etl.data.datasource.xmlhandlers.entryclasses.EntryRelationship;

public class ActEntry extends Entry {
	
	private Map<String,String> templateId;
	private Map<String,String> id;
	private Map<String,String> code;
	private Map<String,String> statusCode;
	private EffectiveTime effectiveTime;
	private EntryRelationship entryRelationShip;
	
	
}
