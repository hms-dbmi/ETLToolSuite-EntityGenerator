package etl.jobs.mappings;

import java.util.HashMap;
import java.util.Map;

import etl.jobs.csv.bdc.BDCJob;

public class vcfIndex extends BDCJob {

	private String filename;	
	
	private String chromosome;	
	
	private String annotated;	
	
	private String gzip;	
	
	private String sampleIds;	
	
	private String participantIds;
	
	private String sampleRelationship;	
	
	private String relatedSampleIds;

	private class VcfIndexException extends Exception {
		public VcfIndexException(String errorMessage) {
			super(errorMessage);
		}
	}
	
	public Map<String,String> buildingSampleIdtoParticipantIdMap(String sampleIds, String participantIds) throws VcfIndexException {
	
		Map<String,String> map = new HashMap<>();
		
		String[] sampleArr = sampleIds.split(",");
		
		String[] participantsArr = participantIds.split(",");
		
		if(sampleArr.length != participantsArr.length) {
			
			if(sampleArr.length > participantsArr.length) {
				
				throw new VcfIndexException("VCF Index invalidToo sample ids > than participants.");

			} else {
				throw new VcfIndexException("VCF Index invalidToo sample ids < than participants.");

			}
			
		}
		return map;

		
		
	}


	public vcfIndex(String filename, String chromosome, String annotated, String gzip, String sampleIds,
			String participantIds, String sampleRelationship, String relatedSampleIds) {
		super();
		this.filename = filename;
		this.chromosome = chromosome;
		this.annotated = annotated;
		this.gzip = gzip;
		this.sampleIds = sampleIds;
		this.participantIds = participantIds;
		this.sampleRelationship = sampleRelationship;
		this.relatedSampleIds = relatedSampleIds;
	}


	public String getFilename() {
		return filename;
	}


	public void setFilename(String filename) {
		this.filename = filename;
	}


	public String getChromosome() {
		return chromosome;
	}


	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}


	public String getAnnotated() {
		return annotated;
	}


	public void setAnnotated(String annotated) {
		this.annotated = annotated;
	}


	public String getGzip() {
		return gzip;
	}


	public void setGzip(String gzip) {
		this.gzip = gzip;
	}


	public String getSampleIds() {
		return sampleIds;
	}


	public void setSampleIds(String sampleIds) {
		this.sampleIds = sampleIds;
	}


	public String getParticipantIds() {
		return participantIds;
	}


	public void setParticipantIds(String participantIds) {
		this.participantIds = participantIds;
	}


	public String getSampleRelationship() {
		return sampleRelationship;
	}


	public void setSampleRelationship(String sampleRelationship) {
		this.sampleRelationship = sampleRelationship;
	}


	public String getRelatedSampleIds() {
		return relatedSampleIds;
	}


	public void setRelatedSampleIds(String relatedSampleIds) {
		this.relatedSampleIds = relatedSampleIds;
	}
	

	
}
