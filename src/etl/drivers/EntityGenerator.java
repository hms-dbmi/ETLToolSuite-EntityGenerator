package etl.drivers;

import java.util.Collection;
import java.util.Set;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.ObservationFact;
import etl.jobs.csv.CEIsequencer;
import etl.jobs.csv.ConceptGenerator;
import etl.jobs.csv.FactGenerator;
import etl.jobs.csv.Job;
import etl.jobs.csv.MetadataGenerator;
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
		
		CEIsequencer.main(args, facts, setCds, jobProperties);
		System.out.println("Processing metadata");
	
		Set<I2B2> metadata = MetadataGenerator.main(args, setCds, jobProperties);
		
		if(metadata == null) throw new Exception("No metadata exists");
		
		System.out.println("Sequencing Data");
		
		System.out.println("Writing Entities");
	
		// write data
		ConceptGenerator.writeConcepts(setCds);
		
		FactGenerator.writeFacts(facts);
		
		MetadataGenerator.writeMetadata(metadata);		
	}
	
}
