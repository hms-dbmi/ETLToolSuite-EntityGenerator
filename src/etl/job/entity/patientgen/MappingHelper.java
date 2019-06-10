package etl.job.entity.patientgen;

import java.util.ArrayList;
import java.util.List;

public class MappingHelper {
	private String AttributeKey = "";
	private List<String> fileKeys = new ArrayList<String>();
	
	public String getAttributeKey() {
		return AttributeKey;
	}
	public void setAttributeKey(String attributeKey) {
		AttributeKey = attributeKey;
	}
	public List<String> getFileKeys() {
		return fileKeys;
	}
	public void setFileKeys(List<String> fileKeys) {
		this.fileKeys = fileKeys;
	}
		
}
