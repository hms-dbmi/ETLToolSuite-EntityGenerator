package etl.jobs.csv;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.I2B2Secure;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.jobproperties.JobProperties;

/**
 * This class will run all necessary entity generators
 * to load into an I2B2/Transmart data model.
 * 
 * @author Thomas Desain
 *
 */
public class EntityGenerator extends Job {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8928347774953895428L;

	/**
	 * Main driver 
	 * 
	 * 
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			execute(args,buildProperties(args));
		} catch (Exception e) {
			System.err.println(e);
		}
	
	}
	/**
	 * Primary wrapper to handle classes subroutines 
	 * 
	 * @param args
	 * @param jobProperties
	 * @throws Exception
	 */
	private static void execute(String[] args, JobProperties jobProperties) throws Exception {
		if(!Files.exists(Paths.get(PROCESSING_FOLDER))) {
			Files.createDirectory(Paths.get(PROCESSING_FOLDER));
		} if(!Files.exists(Paths.get(WRITE_DIR))) {
			Files.createDirectory(Paths.get(WRITE_DIR));
		}
		System.out.println("Building Concepts");
		Set<ConceptDimension> setCds = ConceptGenerator.main(args, jobProperties);
		
		if(setCds == null) throw new Exception("No concepts exists"); 
		System.out.println("Building Facts");
		Set<ObservationFact> facts = FactGenerator.main(args, jobProperties);
		
		if(facts == null) throw new Exception("No facts exists");
		System.out.println("Seqeuncing Data");
		CEIsequencer.main(args, facts, setCds, jobProperties);

		System.out.println("Removing invalid Facts");
		facts = purgeBadFacts(facts);
		
		System.out.println("Building Metadata");
		Set<I2B2> metadata = MetadataGenerator.main(args, setCds, jobProperties);
		
		Set<I2B2Secure> metadataSecure = MetadataGenerator.generateI2B2Secure(metadata);
		
		if(metadata == null) throw new Exception("No metadata exists");
		System.out.println("Writing Concepts");
		ConceptGenerator.writeConcepts(setCds, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		System.out.println("Writing Facts");
		FactGenerator.writeFacts(facts, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		System.out.println("Writing Metadata");
		MetadataGenerator.writeMetadata(metadata, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
		MetadataGenerator.writeMetadataSecure(metadataSecure, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}
	
	private static void cleanupProcessingDir() {
		for(File f: new File(PROCESSING_FOLDER).listFiles()) {
			f.delete();
		}
	}
	private static Set<ObservationFact> purgeBadFacts(Set<ObservationFact> facts) {
		
		Set<ObservationFact> cleaned = new HashSet<ObservationFact>();
		
		for(ObservationFact fact: facts) {
			
			if(!fact.getPatientNum().equals("DELETE")) {
				
				if(fact.getValtypeCd().equalsIgnoreCase("t")) {
					
					if(!fact.getTvalChar().equalsIgnoreCase("null") && !fact.getTvalChar().isEmpty()) {
						if(fact.getTvalChar().contains("null"))System.out.println(fact.getTvalChar());
						cleaned.add(fact);
						
					} 
						
				} else if(fact.getValtypeCd().equalsIgnoreCase("n")) {
					
					if(!fact.getNvalNum().equalsIgnoreCase("null") && !fact.getNvalNum().isEmpty()){
						
						cleaned.add(fact);
						
					}
				}
			}
			
		} 
		return cleaned;
		
	}
	
}
