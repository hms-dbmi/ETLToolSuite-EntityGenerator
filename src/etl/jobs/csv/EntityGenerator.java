package etl.jobs.csv;

import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Set;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.jobproperties.JobProperties;

public class EntityGenerator extends Job {
	
	public static void main(String[] args) {
		
		try {
			execute(args,buildProperties(args));
		} catch (Exception e) {
			System.err.println(e);
		}
	
	}

	private static void execute(String[] args, JobProperties jobProperties) throws Exception {
		System.out.println("Processing Concepts");
		Collection<ConceptDimension> setCds = ConceptGenerator.main(args, jobProperties);
		
		if(setCds == null) throw new Exception("No concepts exists");
		System.out.println("Processing facts");
		Set<ObservationFact> facts = FactGenerator.main(args, jobProperties);
		
		if(facts == null) throw new Exception("No facts exists");
		
		System.out.println("Sequencing Data");
		CEIsequencer.main(args, facts, setCds, jobProperties);
		
		System.out.println("Processing metadata");
	
		Set<I2B2> metadata = MetadataGenerator.main(args, setCds, jobProperties);
		
		if(metadata == null) throw new Exception("No metadata exists");
		
		System.out.println("Writing Entities");
	
		// write data
		ConceptGenerator.writeConcepts(setCds, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
		FactGenerator.writeFacts(facts, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		
		MetadataGenerator.writeMetadata(metadata, StandardOpenOption.CREATE, StandardOpenOption.APPEND);		
	}
	
}