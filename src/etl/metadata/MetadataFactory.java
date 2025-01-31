package etl.metadata;

import java.io.File;
import java.io.IOException;
import java.util.List;

import etl.etlinputs.managedinputs.ManagedInput;
import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.metadata.bdc.BDCMetadata;
import etl.metadata.bdc.GenericBDCMetadata;

public class MetadataFactory {
	
	
	public static Metadata buildMetadata(String type) {
		if("BDC".equalsIgnoreCase(type)) return new BDCMetadata();
		
		return null;
	}


	public static BDCMetadata buildMetadata(String type, List<BDCManagedInput> managedInputs, File metadata) throws IOException {
		if("BDC".equalsIgnoreCase(type)) return new BDCMetadata(managedInputs, metadata);
		if("BDC_GENERIC".equalsIgnoreCase(type)) return new GenericBDCMetadata(managedInputs, metadata);
		return null;
	}
	
	
	public static BDCMetadata readMetadata(String type, File file) {
		if("BDC".equalsIgnoreCase(type)) return BDCMetadata.readMetadata(file);
		
		return null;
	}
}
