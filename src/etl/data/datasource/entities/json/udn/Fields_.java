
package etl.data.datasource.entities.json.udn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "accessionnumber",
    "barcode1",
    "barcode2",
    "core",
    "created_by",
    "created_with_session_key",
    "dateprior",
    "dnaextra",
    "dnaextracted",
    "dnaquality",
    "dnaquantity",
    "dnareceived",
    "dnasend",
    "dnasource",
    "dnasourceother",
    "extractionmethod",
    "has_prior_sequencing",
    "labname",
    "labprior",
    "labpriormany",
    "modified_by",
    "modified_with_session_key",
    "notes",
    "participantcontact",
    "patient",
    "previous_sequencing_types",
    "primarycarecontact",
    "questions_contact",
    "rationale",
    "rationaleexome",
    "rationalewhole",
    "reanalyze",
    "sampleid",
    "sangerrequests",
    "sequencereports",
    "sequencingfiles",
    "submit",
    "testrequested",
    "testrequestedother",
    "trackingnumber",
    "typeofsequencing",
    "yearprior",
    "yearpriorisknown"
})
public class Fields_ {

    @JsonProperty("accessionnumber")
    private String accessionnumber;
    @JsonProperty("barcode1")
    private String barcode1;
    @JsonProperty("barcode2")
    private String barcode2;
    @JsonProperty("core")
    private String core;
    @JsonProperty("created_by")
    private List<String> createdBy = null;
    @JsonProperty("created_with_session_key")
    private Object createdWithSessionKey;
    @JsonProperty("dateprior")
    private Object dateprior;
    @JsonProperty("dnaextra")
    private Object dnaextra;
    @JsonProperty("dnaextracted")
    private String dnaextracted;
    @JsonProperty("dnaquality")
    private String dnaquality;
    @JsonProperty("dnaquantity")
    private String dnaquantity;
    @JsonProperty("dnareceived")
    private String dnareceived;
    @JsonProperty("dnasend")
    private String dnasend;
    @JsonProperty("dnasource")
    private String dnasource;
    @JsonProperty("dnasourceother")
    private String dnasourceother;
    @JsonProperty("extractionmethod")
    private String extractionmethod;
    @JsonProperty("has_prior_sequencing")
    private Object hasPriorSequencing;
    @JsonProperty("labname")
    private String labname;
    @JsonProperty("labprior")
    private String labprior;
    @JsonProperty("labpriormany")
    private List<Object> labpriormany = null;
    @JsonProperty("modified_by")
    private List<String> modifiedBy = null;
    @JsonProperty("modified_with_session_key")
    private Object modifiedWithSessionKey;
    @JsonProperty("notes")
    private String notes;
    @JsonProperty("participantcontact")
    private Object participantcontact;
    @JsonProperty("patient")
    private String patient;
    @JsonProperty("previous_sequencing_types")
    private String previousSequencingTypes;
    @JsonProperty("primarycarecontact")
    private Object primarycarecontact;
    @JsonProperty("questions_contact")
    private Object questionsContact;
    @JsonProperty("rationale")
    private String rationale;
    @JsonProperty("rationaleexome")
    private String rationaleexome;
    @JsonProperty("rationalewhole")
    private String rationalewhole;
    @JsonProperty("reanalyze")
    private Boolean reanalyze;
    @JsonProperty("sampleid")
    private String sampleid;
    @JsonProperty("sangerrequests")
    private List<Integer> sangerrequests = null;
    @JsonProperty("sequencereports")
    private List<Integer> sequencereports = null;
    @JsonProperty("sequencingfiles")
    private List<List<String>> sequencingfiles = null;
    @JsonProperty("submit")
    private Boolean submit;
    @JsonProperty("testrequested")
    private String testrequested;
    @JsonProperty("testrequestedother")
    private String testrequestedother;
    @JsonProperty("trackingnumber")
    private String trackingnumber;
    @JsonProperty("typeofsequencing")
    private String typeofsequencing;
    @JsonProperty("yearprior")
    private Object yearprior;
    @JsonProperty("yearpriorisknown")
    private Object yearpriorisknown;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("accessionnumber")
    public String getAccessionnumber() {
        return accessionnumber;
    }

    @JsonProperty("accessionnumber")
    public void setAccessionnumber(String accessionnumber) {
        this.accessionnumber = accessionnumber;
    }

    @JsonProperty("barcode1")
    public String getBarcode1() {
        return barcode1;
    }

    @JsonProperty("barcode1")
    public void setBarcode1(String barcode1) {
        this.barcode1 = barcode1;
    }

    @JsonProperty("barcode2")
    public String getBarcode2() {
        return barcode2;
    }

    @JsonProperty("barcode2")
    public void setBarcode2(String barcode2) {
        this.barcode2 = barcode2;
    }

    @JsonProperty("core")
    public String getCore() {
        return core;
    }

    @JsonProperty("core")
    public void setCore(String core) {
        this.core = core;
    }

    @JsonProperty("created_by")
    public List<String> getCreatedBy() {
        return createdBy;
    }

    @JsonProperty("created_by")
    public void setCreatedBy(List<String> createdBy) {
        this.createdBy = createdBy;
    }

    @JsonProperty("created_with_session_key")
    public Object getCreatedWithSessionKey() {
        return createdWithSessionKey;
    }

    @JsonProperty("created_with_session_key")
    public void setCreatedWithSessionKey(Object createdWithSessionKey) {
        this.createdWithSessionKey = createdWithSessionKey;
    }

    @JsonProperty("dateprior")
    public Object getDateprior() {
        return dateprior;
    }

    @JsonProperty("dateprior")
    public void setDateprior(Object dateprior) {
        this.dateprior = dateprior;
    }

    @JsonProperty("dnaextra")
    public Object getDnaextra() {
        return dnaextra;
    }

    @JsonProperty("dnaextra")
    public void setDnaextra(Object dnaextra) {
        this.dnaextra = dnaextra;
    }

    @JsonProperty("dnaextracted")
    public String getDnaextracted() {
        return dnaextracted;
    }

    @JsonProperty("dnaextracted")
    public void setDnaextracted(String dnaextracted) {
        this.dnaextracted = dnaextracted;
    }

    @JsonProperty("dnaquality")
    public String getDnaquality() {
        return dnaquality;
    }

    @JsonProperty("dnaquality")
    public void setDnaquality(String dnaquality) {
        this.dnaquality = dnaquality;
    }

    @JsonProperty("dnaquantity")
    public String getDnaquantity() {
        return dnaquantity;
    }

    @JsonProperty("dnaquantity")
    public void setDnaquantity(String dnaquantity) {
        this.dnaquantity = dnaquantity;
    }

    @JsonProperty("dnareceived")
    public String getDnareceived() {
        return dnareceived;
    }

    @JsonProperty("dnareceived")
    public void setDnareceived(String dnareceived) {
        this.dnareceived = dnareceived;
    }

    @JsonProperty("dnasend")
    public String getDnasend() {
        return dnasend;
    }

    @JsonProperty("dnasend")
    public void setDnasend(String dnasend) {
        this.dnasend = dnasend;
    }

    @JsonProperty("dnasource")
    public String getDnasource() {
        return dnasource;
    }

    @JsonProperty("dnasource")
    public void setDnasource(String dnasource) {
        this.dnasource = dnasource;
    }

    @JsonProperty("dnasourceother")
    public String getDnasourceother() {
        return dnasourceother;
    }

    @JsonProperty("dnasourceother")
    public void setDnasourceother(String dnasourceother) {
        this.dnasourceother = dnasourceother;
    }

    @JsonProperty("extractionmethod")
    public String getExtractionmethod() {
        return extractionmethod;
    }

    @JsonProperty("extractionmethod")
    public void setExtractionmethod(String extractionmethod) {
        this.extractionmethod = extractionmethod;
    }

    @JsonProperty("has_prior_sequencing")
    public Object getHasPriorSequencing() {
        return hasPriorSequencing;
    }

    @JsonProperty("has_prior_sequencing")
    public void setHasPriorSequencing(Object hasPriorSequencing) {
        this.hasPriorSequencing = hasPriorSequencing;
    }

    @JsonProperty("labname")
    public String getLabname() {
        return labname;
    }

    @JsonProperty("labname")
    public void setLabname(String labname) {
        this.labname = labname;
    }

    @JsonProperty("labprior")
    public String getLabprior() {
        return labprior;
    }

    @JsonProperty("labprior")
    public void setLabprior(String labprior) {
        this.labprior = labprior;
    }

    @JsonProperty("labpriormany")
    public List<Object> getLabpriormany() {
        return labpriormany;
    }

    @JsonProperty("labpriormany")
    public void setLabpriormany(List<Object> labpriormany) {
        this.labpriormany = labpriormany;
    }

    @JsonProperty("modified_by")
    public List<String> getModifiedBy() {
        return modifiedBy;
    }

    @JsonProperty("modified_by")
    public void setModifiedBy(List<String> modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    @JsonProperty("modified_with_session_key")
    public Object getModifiedWithSessionKey() {
        return modifiedWithSessionKey;
    }

    @JsonProperty("modified_with_session_key")
    public void setModifiedWithSessionKey(Object modifiedWithSessionKey) {
        this.modifiedWithSessionKey = modifiedWithSessionKey;
    }

    @JsonProperty("notes")
    public String getNotes() {
        return notes;
    }

    @JsonProperty("notes")
    public void setNotes(String notes) {
        this.notes = notes;
    }

    @JsonProperty("participantcontact")
    public Object getParticipantcontact() {
        return participantcontact;
    }

    @JsonProperty("participantcontact")
    public void setParticipantcontact(Object participantcontact) {
        this.participantcontact = participantcontact;
    }

    @JsonProperty("patient")
    public String getPatient() {
        return patient;
    }

    @JsonProperty("patient")
    public void setPatient(String patient) {
        this.patient = patient;
    }

    @JsonProperty("previous_sequencing_types")
    public String getPreviousSequencingTypes() {
        return previousSequencingTypes;
    }

    @JsonProperty("previous_sequencing_types")
    public void setPreviousSequencingTypes(String previousSequencingTypes) {
        this.previousSequencingTypes = previousSequencingTypes;
    }

    @JsonProperty("primarycarecontact")
    public Object getPrimarycarecontact() {
        return primarycarecontact;
    }

    @JsonProperty("primarycarecontact")
    public void setPrimarycarecontact(Object primarycarecontact) {
        this.primarycarecontact = primarycarecontact;
    }

    @JsonProperty("questions_contact")
    public Object getQuestionsContact() {
        return questionsContact;
    }

    @JsonProperty("questions_contact")
    public void setQuestionsContact(Object questionsContact) {
        this.questionsContact = questionsContact;
    }

    @JsonProperty("rationale")
    public String getRationale() {
        return rationale;
    }

    @JsonProperty("rationale")
    public void setRationale(String rationale) {
        this.rationale = rationale;
    }

    @JsonProperty("rationaleexome")
    public String getRationaleexome() {
        return rationaleexome;
    }

    @JsonProperty("rationaleexome")
    public void setRationaleexome(String rationaleexome) {
        this.rationaleexome = rationaleexome;
    }

    @JsonProperty("rationalewhole")
    public String getRationalewhole() {
        return rationalewhole;
    }

    @JsonProperty("rationalewhole")
    public void setRationalewhole(String rationalewhole) {
        this.rationalewhole = rationalewhole;
    }

    @JsonProperty("reanalyze")
    public Boolean getReanalyze() {
        return reanalyze;
    }

    @JsonProperty("reanalyze")
    public void setReanalyze(Boolean reanalyze) {
        this.reanalyze = reanalyze;
    }

    @JsonProperty("sampleid")
    public String getSampleid() {
        return sampleid;
    }

    @JsonProperty("sampleid")
    public void setSampleid(String sampleid) {
        this.sampleid = sampleid;
    }

    @JsonProperty("sangerrequests")
    public List<Integer> getSangerrequests() {
        return sangerrequests;
    }

    @JsonProperty("sangerrequests")
    public void setSangerrequests(List<Integer> sangerrequests) {
        this.sangerrequests = sangerrequests;
    }

    @JsonProperty("sequencereports")
    public List<Integer> getSequencereports() {
        return sequencereports;
    }

    @JsonProperty("sequencereports")
    public void setSequencereports(List<Integer> sequencereports) {
        this.sequencereports = sequencereports;
    }

    @JsonProperty("sequencingfiles")
    public List<List<String>> getSequencingfiles() {
        return sequencingfiles;
    }

    @JsonProperty("sequencingfiles")
    public void setSequencingfiles(List<List<String>> sequencingfiles) {
        this.sequencingfiles = sequencingfiles;
    }

    @JsonProperty("submit")
    public Boolean getSubmit() {
        return submit;
    }

    @JsonProperty("submit")
    public void setSubmit(Boolean submit) {
        this.submit = submit;
    }

    @JsonProperty("testrequested")
    public String getTestrequested() {
        return testrequested;
    }

    @JsonProperty("testrequested")
    public void setTestrequested(String testrequested) {
        this.testrequested = testrequested;
    }

    @JsonProperty("testrequestedother")
    public String getTestrequestedother() {
        return testrequestedother;
    }

    @JsonProperty("testrequestedother")
    public void setTestrequestedother(String testrequestedother) {
        this.testrequestedother = testrequestedother;
    }

    @JsonProperty("trackingnumber")
    public String getTrackingnumber() {
        return trackingnumber;
    }

    @JsonProperty("trackingnumber")
    public void setTrackingnumber(String trackingnumber) {
        this.trackingnumber = trackingnumber;
    }

    @JsonProperty("typeofsequencing")
    public String getTypeofsequencing() {
        return typeofsequencing;
    }

    @JsonProperty("typeofsequencing")
    public void setTypeofsequencing(String typeofsequencing) {
        this.typeofsequencing = typeofsequencing;
    }

    @JsonProperty("yearprior")
    public Object getYearprior() {
        return yearprior;
    }

    @JsonProperty("yearprior")
    public void setYearprior(Object yearprior) {
        this.yearprior = yearprior;
    }

    @JsonProperty("yearpriorisknown")
    public Object getYearpriorisknown() {
        return yearpriorisknown;
    }

    @JsonProperty("yearpriorisknown")
    public void setYearpriorisknown(Object yearpriorisknown) {
        this.yearpriorisknown = yearpriorisknown;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
