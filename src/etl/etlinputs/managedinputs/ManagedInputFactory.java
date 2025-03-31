package etl.etlinputs.managedinputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;

public class ManagedInputFactory {

	public static List<BDCManagedInput> buildManagedInputs(String type, List<String[]> managedInputs) {
		if("BDC".equalsIgnoreCase(type)) return BDCManagedInput.buildAll(managedInputs);
	
		
		return null;
	}	
	


	/**
	 * reads the Managed Input File
	 * @return
	 * @throws IOException
	 */
	public static List<BDCManagedInput> readManagedInput(String type, String managedInputFileUrl) throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(managedInputFileUrl))) {
			
			@SuppressWarnings("resource")
			List<String[]> records = new CSVReader(buffer).readAll();

			if("BDC".equalsIgnoreCase(type)) 
				return ManagedInputFactory.buildManagedInputs(type, records);
			else 
				return null;
			
		}
	}

	/**
	 * reads the Genomic Managed Input File
	 * 
	 * @return
	 * @throws IOException
	 */

}
