
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
    "allergies",
    "apgar",
    "clinical-diagnosis",
    "clinicalStatus",
    "contact",
    "date",
    "date_of_birth",
    "date_of_death",
    "disorders",
    "ethnicity",
    "external_id",
    "family_history",
    "features",
    "genes",
    "global_age_of_onset",
    "id",
    "last_modification_date",
    "last_modified_by",
    "life_status",
    "links",
    "meta",
    "nonstandard_features",
    "notes",
    "patient_name",
    "prenatal_perinatal_history",
    "report_id",
    "reporter",
    "sex",
    "solved",
    "specificity",
    "variants"
})
public class Phenotips {

    @JsonProperty("allergies")
    private List<String> allergies = null;
    @JsonProperty("apgar")
    private Apgar apgar;
    @JsonProperty("clinical-diagnosis")
    private List<Object> clinicalDiagnosis = null;
    @JsonProperty("clinicalStatus")
    private String clinicalStatus;
    @JsonProperty("contact")
    private List<Contact> contact = null;
    @JsonProperty("date")
    private String date;
    @JsonProperty("date_of_birth")
    private DateOfBirth dateOfBirth;
    @JsonProperty("date_of_death")
    private DateOfDeath dateOfDeath;
    @JsonProperty("disorders")
    private List<Object> disorders = null;
    @JsonProperty("ethnicity")
    private Ethnicity ethnicity;
    @JsonProperty("external_id")
    private String externalId;
    @JsonProperty("family_history")
    private FamilyHistory familyHistory;
    @JsonProperty("features")
    private List<Feature> features = null;
    @JsonProperty("genes")
    private List<Gene> genes = null;
    @JsonProperty("global_age_of_onset")
    private List<GlobalAgeOfOnset> globalAgeOfOnset = null;
    @JsonProperty("id")
    private String id;
    @JsonProperty("last_modification_date")
    private String lastModificationDate;
    @JsonProperty("last_modified_by")
    private String lastModifiedBy;
    @JsonProperty("life_status")
    private String lifeStatus;
    @JsonProperty("links")
    private List<Link> links = null;
    @JsonProperty("meta")
    private Meta meta;
    @JsonProperty("nonstandard_features")
    private List<Object> nonstandardFeatures = null;
    @JsonProperty("notes")
    private Notes notes;
    @JsonProperty("patient_name")
    private PatientName patientName;
    @JsonProperty("prenatal_perinatal_history")
    private PrenatalPerinatalHistory prenatalPerinatalHistory;
    @JsonProperty("report_id")
    private String reportId;
    @JsonProperty("reporter")
    private String reporter;
    @JsonProperty("sex")
    private String sex;
    @JsonProperty("solved")
    private Solved solved;
    @JsonProperty("specificity")
    private Specificity specificity;
    @JsonProperty("variants")
    private List<Variant> variants = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("allergies")
    public List<String> getAllergies() {
        return allergies;
    }

    @JsonProperty("allergies")
    public void setAllergies(List<String> allergies) {
        this.allergies = allergies;
    }

    @JsonProperty("apgar")
    public Apgar getApgar() {
        return apgar;
    }

    @JsonProperty("apgar")
    public void setApgar(Apgar apgar) {
        this.apgar = apgar;
    }

    @JsonProperty("clinical-diagnosis")
    public List<Object> getClinicalDiagnosis() {
        return clinicalDiagnosis;
    }

    @JsonProperty("clinical-diagnosis")
    public void setClinicalDiagnosis(List<Object> clinicalDiagnosis) {
        this.clinicalDiagnosis = clinicalDiagnosis;
    }

    @JsonProperty("clinicalStatus")
    public String getClinicalStatus() {
        return clinicalStatus;
    }

    @JsonProperty("clinicalStatus")
    public void setClinicalStatus(String clinicalStatus) {
        this.clinicalStatus = clinicalStatus;
    }

    @JsonProperty("contact")
    public List<Contact> getContact() {
        return contact;
    }

    @JsonProperty("contact")
    public void setContact(List<Contact> contact) {
        this.contact = contact;
    }

    @JsonProperty("date")
    public String getDate() {
        return date;
    }

    @JsonProperty("date")
    public void setDate(String date) {
        this.date = date;
    }

    @JsonProperty("date_of_birth")
    public DateOfBirth getDateOfBirth() {
        return dateOfBirth;
    }

    @JsonProperty("date_of_birth")
    public void setDateOfBirth(DateOfBirth dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    @JsonProperty("date_of_death")
    public DateOfDeath getDateOfDeath() {
        return dateOfDeath;
    }

    @JsonProperty("date_of_death")
    public void setDateOfDeath(DateOfDeath dateOfDeath) {
        this.dateOfDeath = dateOfDeath;
    }

    @JsonProperty("disorders")
    public List<Object> getDisorders() {
        return disorders;
    }

    @JsonProperty("disorders")
    public void setDisorders(List<Object> disorders) {
        this.disorders = disorders;
    }

    @JsonProperty("ethnicity")
    public Ethnicity getEthnicity() {
        return ethnicity;
    }

    @JsonProperty("ethnicity")
    public void setEthnicity(Ethnicity ethnicity) {
        this.ethnicity = ethnicity;
    }

    @JsonProperty("external_id")
    public String getExternalId() {
        return externalId;
    }

    @JsonProperty("external_id")
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    @JsonProperty("family_history")
    public FamilyHistory getFamilyHistory() {
        return familyHistory;
    }

    @JsonProperty("family_history")
    public void setFamilyHistory(FamilyHistory familyHistory) {
        this.familyHistory = familyHistory;
    }

    @JsonProperty("features")
    public List<Feature> getFeatures() {
        return features;
    }

    @JsonProperty("features")
    public void setFeatures(List<Feature> features) {
        this.features = features;
    }

    @JsonProperty("genes")
    public List<Gene> getGenes() {
        return genes;
    }

    @JsonProperty("genes")
    public void setGenes(List<Gene> genes) {
        this.genes = genes;
    }

    @JsonProperty("global_age_of_onset")
    public List<GlobalAgeOfOnset> getGlobalAgeOfOnset() {
        return globalAgeOfOnset;
    }

    @JsonProperty("global_age_of_onset")
    public void setGlobalAgeOfOnset(List<GlobalAgeOfOnset> globalAgeOfOnset) {
        this.globalAgeOfOnset = globalAgeOfOnset;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("last_modification_date")
    public String getLastModificationDate() {
        return lastModificationDate;
    }

    @JsonProperty("last_modification_date")
    public void setLastModificationDate(String lastModificationDate) {
        this.lastModificationDate = lastModificationDate;
    }

    @JsonProperty("last_modified_by")
    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    @JsonProperty("last_modified_by")
    public void setLastModifiedBy(String lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
    }

    @JsonProperty("life_status")
    public String getLifeStatus() {
        return lifeStatus;
    }

    @JsonProperty("life_status")
    public void setLifeStatus(String lifeStatus) {
        this.lifeStatus = lifeStatus;
    }

    @JsonProperty("links")
    public List<Link> getLinks() {
        return links;
    }

    @JsonProperty("links")
    public void setLinks(List<Link> links) {
        this.links = links;
    }

    @JsonProperty("meta")
    public Meta getMeta() {
        return meta;
    }

    @JsonProperty("meta")
    public void setMeta(Meta meta) {
        this.meta = meta;
    }

    @JsonProperty("nonstandard_features")
    public List<Object> getNonstandardFeatures() {
        return nonstandardFeatures;
    }

    @JsonProperty("nonstandard_features")
    public void setNonstandardFeatures(List<Object> nonstandardFeatures) {
        this.nonstandardFeatures = nonstandardFeatures;
    }

    @JsonProperty("notes")
    public Notes getNotes() {
        return notes;
    }

    @JsonProperty("notes")
    public void setNotes(Notes notes) {
        this.notes = notes;
    }

    @JsonProperty("patient_name")
    public PatientName getPatientName() {
        return patientName;
    }

    @JsonProperty("patient_name")
    public void setPatientName(PatientName patientName) {
        this.patientName = patientName;
    }

    @JsonProperty("prenatal_perinatal_history")
    public PrenatalPerinatalHistory getPrenatalPerinatalHistory() {
        return prenatalPerinatalHistory;
    }

    @JsonProperty("prenatal_perinatal_history")
    public void setPrenatalPerinatalHistory(PrenatalPerinatalHistory prenatalPerinatalHistory) {
        this.prenatalPerinatalHistory = prenatalPerinatalHistory;
    }

    @JsonProperty("report_id")
    public String getReportId() {
        return reportId;
    }

    @JsonProperty("report_id")
    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    @JsonProperty("reporter")
    public String getReporter() {
        return reporter;
    }

    @JsonProperty("reporter")
    public void setReporter(String reporter) {
        this.reporter = reporter;
    }

    @JsonProperty("sex")
    public String getSex() {
        return sex;
    }

    @JsonProperty("sex")
    public void setSex(String sex) {
        this.sex = sex;
    }

    @JsonProperty("solved")
    public Solved getSolved() {
        return solved;
    }

    @JsonProperty("solved")
    public void setSolved(Solved solved) {
        this.solved = solved;
    }

    @JsonProperty("specificity")
    public Specificity getSpecificity() {
        return specificity;
    }

    @JsonProperty("specificity")
    public void setSpecificity(Specificity specificity) {
        this.specificity = specificity;
    }

    @JsonProperty("variants")
    public List<Variant> getVariants() {
        return variants;
    }

    @JsonProperty("variants")
    public void setVariants(List<Variant> variants) {
        this.variants = variants;
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
