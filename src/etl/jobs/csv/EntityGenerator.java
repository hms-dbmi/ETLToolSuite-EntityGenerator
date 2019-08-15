package etl.jobs.csv;

import java.nio.file.StandardOpenOption;
import java.util.Collection;
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
		Collection<ConceptDimension> setCds = ConceptGenerator.main(args, jobProperties);
		
		if(setCds == null) throw new Exception("No concepts exists"); 
		Set<ObservationFact> facts = FactGenerator.main(args, jobProperties);
		
		if(facts == null) throw new Exception("No facts exists");
		
		CEIsequencer.main(args, facts, setCds, jobProperties);
	
		Set<I2B2> metadata = MetadataGenerator.main(args, setCds, jobProperties);
		
		Set<I2B2Secure> metadataSecure = MetadataGenerator.generateI2B2Secure(metadata);
		
		if(metadata == null) throw new Exception("No metadata exists");

		ConceptGenerator.writeConcepts(setCds, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
		FactGenerator.writeFacts(facts, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
		MetadataGenerator.writeMetadata(metadata, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
		MetadataGenerator.writeMetadataSecure(metadataSecure, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
	}
	
}
