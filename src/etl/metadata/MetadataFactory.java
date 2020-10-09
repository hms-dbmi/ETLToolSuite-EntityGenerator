package etl.metadata;

import java.io.IOException;
import java.util.List;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.metadata.bdc.BDCMetadata;

public class MetadataFactory {
	
	public static Metadata buildMetadata(String type, List<ManagedInput> managedInputs) throws IOException {
		if("BDC".equalsIgnoreCase(type)) return new BDCMetadata(managedInputs);
		
		return null;
	}
	
	
	public static Metadata buildMetadata(String type) {
		if("BDC".equalsIgnoreCase(type)) return new BDCMetadata();
		
		return null;
	}
	
}
