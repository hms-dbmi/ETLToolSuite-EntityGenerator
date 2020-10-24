package etl.job.entity;

import java.util.Properties;

public class ConfigFile {
	
	public String 	filename  = "./data/";
	public String	mappingfile = "./mappings/mapping.part1.csv;";
	public String	patientmappingfile = "./mappings/mapping.csv.patient";
	public String	writedir = "./completed/";
	public String	skipmapperheader = "N";
	public String	skipdataheader = "Y";
	public char	datadelimiter = ',';
	public String	mappingdelimiter = ",";
	public char		mappingquotedstring = '"';
	public String	trialid = "MESA";
	public String	appending = "Y";
	public String	ispartition = "Y";
	public String	finalpartition = "N";
	public String	sequencedata = "N";
	public String	sequenceinstance = "Y";
	public String	sequencepatient = "Y";
	public String	sequenceconcept = "Y";
	public String	sequenceencounter = "Y";
	public String conceptcdstartseq = "0";
	public String encounternumstartseq = "0";
	public String patientnumstartseq = "0";
	public String pathseparator = "µ";
	public String usepatientmapping = "N"; 
	public String patientcol = "0";

	public ConfigFile(Properties prop) {
		super();
		if(prop.containsKey("filename")) this.filename = prop.getProperty("filename");
		if(prop.containsKey("mappingfile")) this.mappingfile = prop.getProperty("mappingfile");
		if(prop.containsKey("patientmappingfile")) this.patientmappingfile = prop.getProperty("patientmappingfile");
		if(prop.containsKey("writedir")) this.writedir = prop.getProperty("writedir");
		if(prop.containsKey("skipmapperheader")) this.skipmapperheader = prop.getProperty("skipmapperheader");
		if(prop.containsKey("skipdataheader")) this.skipdataheader = prop.getProperty("skipdataheader");
		if(prop.containsKey("datadelimiter")) this.datadelimiter = prop.getProperty("datadelimiter").toCharArray()[0];
		if(prop.containsKey("mappingdelimiter")) this.mappingdelimiter = prop.getProperty("mappingdelimiter");
		if(prop.containsKey("mappingquotedstring")) this.mappingquotedstring = prop.getProperty("mappingquotedstring").charAt(0);
		if(prop.containsKey("trialid")) this.trialid = prop.getProperty("trialid");
		if(prop.containsKey("appending")) this.appending = prop.getProperty("appending");
		if(prop.containsKey("ispartition")) this.ispartition = prop.getProperty("ispartition");
		if(prop.containsKey("finalpartition")) this.finalpartition = prop.getProperty("finalpartition");
		if(prop.containsKey("sequencedata")) this.sequencedata = prop.getProperty("sequencedata");
		if(prop.containsKey("sequenceinstance")) this.sequenceinstance = prop.getProperty("sequenceinstance");
		if(prop.containsKey("sequencepatient")) this.sequencepatient = prop.getProperty("sequencepatient");
		if(prop.containsKey("sequenceconcept")) this.sequenceconcept = prop.getProperty("sequenceconcept");
		if(prop.containsKey("sequenceencounter")) this.sequenceencounter = prop.getProperty("sequenceencounter");
		if(prop.containsKey("conceptcdstartseq")) this.conceptcdstartseq = prop.getProperty("conceptcdstartseq");
		if(prop.containsKey("encounternumstartseq")) this.encounternumstartseq = prop.getProperty("encounternumstartseq");
		if(prop.containsKey("patientnumstartseq")) this.patientnumstartseq = prop.getProperty("patientnumstartseq");
		if(prop.containsKey("pathseparator")) this.pathseparator = prop.getProperty("pathseparator");
		if(prop.containsKey("usepatientmapping")) this.usepatientmapping = prop.getProperty("usepatientmapping");
		if(prop.containsKey("patientcol")) this.patientcol = prop.getProperty("patientcol");

	}

	@Override
	public String toString() {
		
		String ddlimit = datadelimiter == '\t' ? "\\t" : new Character(datadelimiter).toString() ;
		
		return "filename=" + filename + "\n" +
				"mappingfile=" + mappingfile + "\n" +
				"patientmappingfile=" + patientmappingfile + "\n" +
				"writedir=" + writedir + "\n" +
				"mappingskipheader=" + skipmapperheader + "\n" +
				"skipdataheader=" + skipdataheader + "\n" +
				"datadelimiter=" + ddlimit + "\n" +
				"mappingdelimiter=" + mappingdelimiter + "\n" +
				"mappingquotedstring=" + mappingquotedstring + "\n" +
				"appending=" + appending + "\n" +
				"ispartition=" + ispartition + "\n" +
				"finalpartition=" + finalpartition + "\n" +
				"sequencedata=" + sequencedata + "\n" +
				"sequenceinstance=" + sequenceinstance + "\n" +
				"sequencepatient=" + sequencepatient + "\n" +
				"sequenceconcept=" + sequenceconcept + "\n" +
				"sequenceencounter=" + sequenceencounter + "\n" +
				"conceptcdstartseq=" + conceptcdstartseq + "\n" +
				"encounternumstartseq=" + encounternumstartseq + "\n" +
				"patientnumstartseq=" + patientnumstartseq + "\n" + 
				"pathseparator=" + pathseparator + "\n" + 
				"usepatientmapping=" + usepatientmapping + "\n" + 
				"patientcol=" + patientcol + "\n" + 
				"trialid=" + trialid + "\n";

	}
	
	
}
