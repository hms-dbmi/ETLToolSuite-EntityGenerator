package etl.data.datasource.xmlhandlers.entryclasses;

import java.util.List;
import java.util.Map;

public class Observation extends XMLEntity{
	
	private Map<String,String> observation;
	private Map<String,String> templateId;
	private Map<String,String> id;
	private Map<String,String> code;
	private Map<String,String> statusCode;
	private EffectiveTime effectiveTime;
	private Map<String,String> value;
	private Participant participant;
	private List<EntryRelationship> entryRelationship;
	private Text text;
	private Map<String,String> interpretationCode;
	
}
