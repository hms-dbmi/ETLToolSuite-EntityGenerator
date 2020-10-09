package etl.etlinputs.managedinputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.metadata.Metadata;
import etl.metadata.bdc.BDCMetadata;

public class ManagedInputFactory {

	public static List<ManagedInput> builManagedInputs(String type, List<String[]> managedInputs) throws IOException {
		if("BDC".equalsIgnoreCase(type)) return BDCManagedInput.buildAll(managedInputs);
		
		return null;
	}	
	
	/**
	 * reads the Managed Input File
	 * @return
	 * @throws IOException
	 */
	public static List<ManagedInput> readManagedInput(String type, String managedInputFileUrl) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(managedInputFileUrl))) {
			
			List<String[]> records = new CSVReader(buffer).readAll();
			
			return ManagedInputFactory.builManagedInputs(type, records);
			
		}
	}
}
