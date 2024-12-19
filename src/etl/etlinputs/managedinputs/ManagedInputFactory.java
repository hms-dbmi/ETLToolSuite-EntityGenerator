package etl.etlinputs.managedinputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.opencsv.CSVReader;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.etlinputs.managedinputs.bdc.BDCGenomicManagedInput;

public class ManagedInputFactory {

	public static List<ManagedInput> buildManagedInputs(String type, List<String[]> managedInputs) throws IOException {
		if("BDC".equalsIgnoreCase(type)) return BDCManagedInput.buildAll(managedInputs);
	
		
		return null;
	}	
	
	public static List<GenomicManagedInput> buildGenomicManagedInputs(String type, List<String[]> genomicManagedInputs) throws IOException {
		if ("BDCGenomic".equalsIgnoreCase(type))
			return BDCGenomicManagedInput.buildAll(genomicManagedInputs);
	
		
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
			
			return ManagedInputFactory.buildManagedInputs(type, records);
			
		}
	}

	/**
	 * reads the Genomic Managed Input File
	 * 
	 * @return
	 * @throws IOException
	 */
	public static List<GenomicManagedInput> readGenomicManagedInput(String type, String genomicManagedInputFileUrl) throws IOException {

		try (BufferedReader buffer = Files.newBufferedReader(Paths.get(genomicManagedInputFileUrl))) {

			List<String[]> records = new CSVReader(buffer).readAll();

			return ManagedInputFactory.buildGenomicManagedInputs(type, records);

		}
	}
}
