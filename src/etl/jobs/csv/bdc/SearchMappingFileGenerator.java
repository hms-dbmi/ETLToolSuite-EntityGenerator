package etl.jobs.csv.bdc;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class SearchMappingFileGenerator extends BDCJob {

	public static void main(String[] args) {
		try {
			
			setVariables(args, buildProperties(args));
			
			//setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}
		execute();
		/*try {
			
			execute();
			
		} catch (IOException e) {
			
			System.err.println(e);
			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}

	private static void execute() {
		
		File dictionaryFile = new File("data/phs001237.v2.pht005989.v2.TOPMed_WGS_WHI_Sample_Attributes.data_dict.xml");
		
		Document dataDic = buildDictionary(dictionaryFile);
		Map<String, Map<String, String>> valueLookup = (dataDic == null) ? null: buildValueLookup(dataDic);

		System.out.println();
	}

}
