
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
    "activestatus",
    "affected",
    "all_races",
    "alternatesite",
    "appliedbefore",
    "birth_assigned_gender",
    "clinicalsite",
    "clinicaltrials",
    "created_by",
    "created_with_session_key",
    "current_gender_identity",
    "dob",
    "doctoraddress1",
    "doctoraddress2",
    "doctoraddressmanual",
    "doctorcity",
    "doctoremail",
    "doctorfax",
    "doctorfirst",
    "doctorlast",
    "doctorphone",
    "doctorstate",
    "doctorzip",
    "encoded_relation",
    "environment",
    "environmentexplain",
    "ethnicity",
    "evaluationdate",
    "exposure",
    "exposureexplain",
    "familyid",
    "gender",
    "geographicexplain",
    "geographicrefer",
    "ifeval",
    "languagepreference",
    "modified_by",
    "modified_with_session_key",
    "other_race",
    "other_symptom_explain",
    "patientaddress1",
    "patientaddress2",
    "patientaddressmanual",
    "patientcity",
    "patientemail",
    "patientemailrep",
    "patientfirst",
    "patientguardian",
    "patientlast",
    "patientphone",
    "patientstate",
    "patientzip",
    "phenotips",
    "phenotipsid",
    "primaryrelative",
    "primaryrelativerelation",
    "qualtricsid",
    "race",
    "seenatclinicalsites",
    "sequence",
    "similarsymptoms",
    "similarsymptomsexplain",
    "simpleid",
    "symptom",
    "symptomonset",
    "travellimitations",
    "travellimitationsmulti",
    "travellimitationsother",
    "udn_referral",
    "udn_referral_method",
    "uuid"
})
public class Fields {

    @JsonProperty("activestatus")
    private String activestatus;
    @JsonProperty("affected")
    private String affected;
    @JsonProperty("all_races")
    private String allRaces;
    @JsonProperty("alternatesite")
    private Object alternatesite;
    @JsonProperty("appliedbefore")
    private String appliedbefore;
    @JsonProperty("birth_assigned_gender")
    private String birthAssignedGender;
    @JsonProperty("clinicalsite")
    private String clinicalsite;
    @JsonProperty("clinicaltrials")
    private String clinicaltrials;
    @JsonProperty("created_by")
    private List<String> createdBy = null;
    @JsonProperty("created_with_session_key")
    private Object createdWithSessionKey;
    @JsonProperty("current_gender_identity")
    private String currentGenderIdentity;
    @JsonProperty("dob")
    private String dob;
    @JsonProperty("doctoraddress1")
    private Object doctoraddress1;
    @JsonProperty("doctoraddress2")
    private Object doctoraddress2;
    @JsonProperty("doctoraddressmanual")
    private Object doctoraddressmanual;
    @JsonProperty("doctorcity")
    private String doctorcity;
    @JsonProperty("doctoremail")
    private Object doctoremail;
    @JsonProperty("doctorfax")
    private Object doctorfax;
    @JsonProperty("doctorfirst")
    private Object doctorfirst;
    @JsonProperty("doctorlast")
    private Object doctorlast;
    @JsonProperty("doctorphone")
    private Object doctorphone;
    @JsonProperty("doctorstate")
    private String doctorstate;
    @JsonProperty("doctorzip")
    private String doctorzip;
    @JsonProperty("encoded_relation")
    private Object encodedRelation;
    @JsonProperty("environment")
    private String environment;
    @JsonProperty("environmentexplain")
    private String environmentexplain;
    @JsonProperty("ethnicity")
    private String ethnicity;
    @JsonProperty("evaluationdate")
    private String evaluationdate;
    @JsonProperty("exposure")
    private String exposure;
    @JsonProperty("exposureexplain")
    private String exposureexplain;
    @JsonProperty("familyid")
    private String familyid;
    @JsonProperty("gender")
    private String gender;
    @JsonProperty("geographicexplain")
    private String geographicexplain;
    @JsonProperty("geographicrefer")
    private String geographicrefer;
    @JsonProperty("ifeval")
    private Boolean ifeval;
    @JsonProperty("languagepreference")
    private String languagepreference;
    @JsonProperty("modified_by")
    private List<String> modifiedBy = null;
    @JsonProperty("modified_with_session_key")
    private Object modifiedWithSessionKey;
    @JsonProperty("other_race")
    private String otherRace;
    @JsonProperty("other_symptom_explain")
    private String otherSymptomExplain;
    @JsonProperty("patientaddress1")
    private Object patientaddress1;
    @JsonProperty("patientaddress2")
    private Object patientaddress2;
    @JsonProperty("patientaddressmanual")
    private Object patientaddressmanual;
    @JsonProperty("patientcity")
    private String patientcity;
    @JsonProperty("patientemail")
    private Object patientemail;
    @JsonProperty("patientemailrep")
    private String patientemailrep;
    @JsonProperty("patientfirst")
    private Object patientfirst;
    @JsonProperty("patientguardian")
    private String patientguardian;
    @JsonProperty("patientlast")
    private Object patientlast;
    @JsonProperty("patientphone")
    private Object patientphone;
    @JsonProperty("patientstate")
    private String patientstate;
    @JsonProperty("patientzip")
    private String patientzip;
    @JsonProperty("phenotips")
    private Phenotips phenotips;
    @JsonProperty("phenotipsid")
    private String phenotipsid;
    @JsonProperty("primaryrelative")
    private Object primaryrelative;
    @JsonProperty("primaryrelativerelation")
    private Object primaryrelativerelation;
    @JsonProperty("qualtricsid")
    private String qualtricsid;
    @JsonProperty("race")
    private List<String> race = null;
    @JsonProperty("seenatclinicalsites")
    private List<String> seenatclinicalsites = null;
    @JsonProperty("sequence")
    private List<Sequence> sequence = null;
    @JsonProperty("similarsymptoms")
    private String similarsymptoms;
    @JsonProperty("similarsymptomsexplain")
    private String similarsymptomsexplain;
    @JsonProperty("simpleid")
    private String simpleid;
    @JsonProperty("symptom")
    private String symptom;
    @JsonProperty("symptomonset")
    private String symptomonset;
    @JsonProperty("travellimitations")
    private Object travellimitations;
    @JsonProperty("travellimitationsmulti")
    private List<Object> travellimitationsmulti = null;
    @JsonProperty("travellimitationsother")
    private String travellimitationsother;
    @JsonProperty("udn_referral")
    private String udnReferral;
    @JsonProperty("udn_referral_method")
    private Object udnReferralMethod;
    @JsonProperty("uuid")
    private String uuid;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("activestatus")
    public String getActivestatus() {
        return activestatus;
    }

    @JsonProperty("activestatus")
    public void setActivestatus(String activestatus) {
        this.activestatus = activestatus;
    }

    @JsonProperty("affected")
    public String getAffected() {
        return affected;
    }

    @JsonProperty("affected")
    public void setAffected(String affected) {
        this.affected = affected;
    }

    @JsonProperty("all_races")
    public String getAllRaces() {
        return allRaces;
    }

    @JsonProperty("all_races")
    public void setAllRaces(String allRaces) {
        this.allRaces = allRaces;
    }

    @JsonProperty("alternatesite")
    public Object getAlternatesite() {
        return alternatesite;
    }

    @JsonProperty("alternatesite")
    public void setAlternatesite(Object alternatesite) {
        this.alternatesite = alternatesite;
    }

    @JsonProperty("appliedbefore")
    public String getAppliedbefore() {
        return appliedbefore;
    }

    @JsonProperty("appliedbefore")
    public void setAppliedbefore(String appliedbefore) {
        this.appliedbefore = appliedbefore;
    }

    @JsonProperty("birth_assigned_gender")
    public String getBirthAssignedGender() {
        return birthAssignedGender;
    }

    @JsonProperty("birth_assigned_gender")
    public void setBirthAssignedGender(String birthAssignedGender) {
        this.birthAssignedGender = birthAssignedGender;
    }

    @JsonProperty("clinicalsite")
    public String getClinicalsite() {
        return clinicalsite;
    }

    @JsonProperty("clinicalsite")
    public void setClinicalsite(String clinicalsite) {
        this.clinicalsite = clinicalsite;
    }

    @JsonProperty("clinicaltrials")
    public String getClinicaltrials() {
        return clinicaltrials;
    }

    @JsonProperty("clinicaltrials")
    public void setClinicaltrials(String clinicaltrials) {
        this.clinicaltrials = clinicaltrials;
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

    @JsonProperty("current_gender_identity")
    public String getCurrentGenderIdentity() {
        return currentGenderIdentity;
    }

    @JsonProperty("current_gender_identity")
    public void setCurrentGenderIdentity(String currentGenderIdentity) {
        this.currentGenderIdentity = currentGenderIdentity;
    }

    @JsonProperty("dob")
    public String getDob() {
        return dob;
    }

    @JsonProperty("dob")
    public void setDob(String dob) {
        this.dob = dob;
    }

    @JsonProperty("doctoraddress1")
    public Object getDoctoraddress1() {
        return doctoraddress1;
    }

    @JsonProperty("doctoraddress1")
    public void setDoctoraddress1(Object doctoraddress1) {
        this.doctoraddress1 = doctoraddress1;
    }

    @JsonProperty("doctoraddress2")
    public Object getDoctoraddress2() {
        return doctoraddress2;
    }

    @JsonProperty("doctoraddress2")
    public void setDoctoraddress2(Object doctoraddress2) {
        this.doctoraddress2 = doctoraddress2;
    }

    @JsonProperty("doctoraddressmanual")
    public Object getDoctoraddressmanual() {
        return doctoraddressmanual;
    }

    @JsonProperty("doctoraddressmanual")
    public void setDoctoraddressmanual(Object doctoraddressmanual) {
        this.doctoraddressmanual = doctoraddressmanual;
    }

    @JsonProperty("doctorcity")
    public String getDoctorcity() {
        return doctorcity;
    }

    @JsonProperty("doctorcity")
    public void setDoctorcity(String doctorcity) {
        this.doctorcity = doctorcity;
    }

    @JsonProperty("doctoremail")
    public Object getDoctoremail() {
        return doctoremail;
    }

    @JsonProperty("doctoremail")
    public void setDoctoremail(Object doctoremail) {
        this.doctoremail = doctoremail;
    }

    @JsonProperty("doctorfax")
    public Object getDoctorfax() {
        return doctorfax;
    }

    @JsonProperty("doctorfax")
    public void setDoctorfax(Object doctorfax) {
        this.doctorfax = doctorfax;
    }

    @JsonProperty("doctorfirst")
    public Object getDoctorfirst() {
        return doctorfirst;
    }

    @JsonProperty("doctorfirst")
    public void setDoctorfirst(Object doctorfirst) {
        this.doctorfirst = doctorfirst;
    }

    @JsonProperty("doctorlast")
    public Object getDoctorlast() {
        return doctorlast;
    }

    @JsonProperty("doctorlast")
    public void setDoctorlast(Object doctorlast) {
        this.doctorlast = doctorlast;
    }

    @JsonProperty("doctorphone")
    public Object getDoctorphone() {
        return doctorphone;
    }

    @JsonProperty("doctorphone")
    public void setDoctorphone(Object doctorphone) {
        this.doctorphone = doctorphone;
    }

    @JsonProperty("doctorstate")
    public String getDoctorstate() {
        return doctorstate;
    }

    @JsonProperty("doctorstate")
    public void setDoctorstate(String doctorstate) {
        this.doctorstate = doctorstate;
    }

    @JsonProperty("doctorzip")
    public String getDoctorzip() {
        return doctorzip;
    }

    @JsonProperty("doctorzip")
    public void setDoctorzip(String doctorzip) {
        this.doctorzip = doctorzip;
    }

    @JsonProperty("encoded_relation")
    public Object getEncodedRelation() {
        return encodedRelation;
    }

    @JsonProperty("encoded_relation")
    public void setEncodedRelation(Object encodedRelation) {
        this.encodedRelation = encodedRelation;
    }

    @JsonProperty("environment")
    public String getEnvironment() {
        return environment;
    }

    @JsonProperty("environment")
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    @JsonProperty("environmentexplain")
    public String getEnvironmentexplain() {
        return environmentexplain;
    }

    @JsonProperty("environmentexplain")
    public void setEnvironmentexplain(String environmentexplain) {
        this.environmentexplain = environmentexplain;
    }

    @JsonProperty("ethnicity")
    public String getEthnicity() {
        return ethnicity;
    }

    @JsonProperty("ethnicity")
    public void setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
    }

    @JsonProperty("evaluationdate")
    public String getEvaluationdate() {
        return evaluationdate;
    }

    @JsonProperty("evaluationdate")
    public void setEvaluationdate(String evaluationdate) {
        this.evaluationdate = evaluationdate;
    }

    @JsonProperty("exposure")
    public String getExposure() {
        return exposure;
    }

    @JsonProperty("exposure")
    public void setExposure(String exposure) {
        this.exposure = exposure;
    }

    @JsonProperty("exposureexplain")
    public String getExposureexplain() {
        return exposureexplain;
    }

    @JsonProperty("exposureexplain")
    public void setExposureexplain(String exposureexplain) {
        this.exposureexplain = exposureexplain;
    }

    @JsonProperty("familyid")
    public String getFamilyid() {
        return familyid;
    }

    @JsonProperty("familyid")
    public void setFamilyid(String familyid) {
        this.familyid = familyid;
    }

    @JsonProperty("gender")
    public String getGender() {
        return gender;
    }

    @JsonProperty("gender")
    public void setGender(String gender) {
        this.gender = gender;
    }

    @JsonProperty("geographicexplain")
    public String getGeographicexplain() {
        return geographicexplain;
    }

    @JsonProperty("geographicexplain")
    public void setGeographicexplain(String geographicexplain) {
        this.geographicexplain = geographicexplain;
    }

    @JsonProperty("geographicrefer")
    public String getGeographicrefer() {
        return geographicrefer;
    }

    @JsonProperty("geographicrefer")
    public void setGeographicrefer(String geographicrefer) {
        this.geographicrefer = geographicrefer;
    }

    @JsonProperty("ifeval")
    public Boolean getIfeval() {
        return ifeval;
    }

    @JsonProperty("ifeval")
    public void setIfeval(Boolean ifeval) {
        this.ifeval = ifeval;
    }

    @JsonProperty("languagepreference")
    public String getLanguagepreference() {
        return languagepreference;
    }

    @JsonProperty("languagepreference")
    public void setLanguagepreference(String languagepreference) {
        this.languagepreference = languagepreference;
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

    @JsonProperty("other_race")
    public String getOtherRace() {
        return otherRace;
    }

    @JsonProperty("other_race")
    public void setOtherRace(String otherRace) {
        this.otherRace = otherRace;
    }

    @JsonProperty("other_symptom_explain")
    public String getOtherSymptomExplain() {
        return otherSymptomExplain;
    }

    @JsonProperty("other_symptom_explain")
    public void setOtherSymptomExplain(String otherSymptomExplain) {
        this.otherSymptomExplain = otherSymptomExplain;
    }

    @JsonProperty("patientaddress1")
    public Object getPatientaddress1() {
        return patientaddress1;
    }

    @JsonProperty("patientaddress1")
    public void setPatientaddress1(Object patientaddress1) {
        this.patientaddress1 = patientaddress1;
    }

    @JsonProperty("patientaddress2")
    public Object getPatientaddress2() {
        return patientaddress2;
    }

    @JsonProperty("patientaddress2")
    public void setPatientaddress2(Object patientaddress2) {
        this.patientaddress2 = patientaddress2;
    }

    @JsonProperty("patientaddressmanual")
    public Object getPatientaddressmanual() {
        return patientaddressmanual;
    }

    @JsonProperty("patientaddressmanual")
    public void setPatientaddressmanual(Object patientaddressmanual) {
        this.patientaddressmanual = patientaddressmanual;
    }

    @JsonProperty("patientcity")
    public String getPatientcity() {
        return patientcity;
    }

    @JsonProperty("patientcity")
    public void setPatientcity(String patientcity) {
        this.patientcity = patientcity;
    }

    @JsonProperty("patientemail")
    public Object getPatientemail() {
        return patientemail;
    }

    @JsonProperty("patientemail")
    public void setPatientemail(Object patientemail) {
        this.patientemail = patientemail;
    }

    @JsonProperty("patientemailrep")
    public String getPatientemailrep() {
        return patientemailrep;
    }

    @JsonProperty("patientemailrep")
    public void setPatientemailrep(String patientemailrep) {
        this.patientemailrep = patientemailrep;
    }

    @JsonProperty("patientfirst")
    public Object getPatientfirst() {
        return patientfirst;
    }

    @JsonProperty("patientfirst")
    public void setPatientfirst(Object patientfirst) {
        this.patientfirst = patientfirst;
    }

    @JsonProperty("patientguardian")
    public String getPatientguardian() {
        return patientguardian;
    }

    @JsonProperty("patientguardian")
    public void setPatientguardian(String patientguardian) {
        this.patientguardian = patientguardian;
    }

    @JsonProperty("patientlast")
    public Object getPatientlast() {
        return patientlast;
    }

    @JsonProperty("patientlast")
    public void setPatientlast(Object patientlast) {
        this.patientlast = patientlast;
    }

    @JsonProperty("patientphone")
    public Object getPatientphone() {
        return patientphone;
    }

    @JsonProperty("patientphone")
    public void setPatientphone(Object patientphone) {
        this.patientphone = patientphone;
    }

    @JsonProperty("patientstate")
    public String getPatientstate() {
        return patientstate;
    }

    @JsonProperty("patientstate")
    public void setPatientstate(String patientstate) {
        this.patientstate = patientstate;
    }

    @JsonProperty("patientzip")
    public String getPatientzip() {
        return patientzip;
    }

    @JsonProperty("patientzip")
    public void setPatientzip(String patientzip) {
        this.patientzip = patientzip;
    }

    @JsonProperty("phenotips")
    public Phenotips getPhenotips() {
        return phenotips;
    }

    @JsonProperty("phenotips")
    public void setPhenotips(Phenotips phenotips) {
        this.phenotips = phenotips;
    }

    @JsonProperty("phenotipsid")
    public String getPhenotipsid() {
        return phenotipsid;
    }

    @JsonProperty("phenotipsid")
    public void setPhenotipsid(String phenotipsid) {
        this.phenotipsid = phenotipsid;
    }

    @JsonProperty("primaryrelative")
    public Object getPrimaryrelative() {
        return primaryrelative;
    }

    @JsonProperty("primaryrelative")
    public void setPrimaryrelative(Object primaryrelative) {
        this.primaryrelative = primaryrelative;
    }

    @JsonProperty("primaryrelativerelation")
    public Object getPrimaryrelativerelation() {
        return primaryrelativerelation;
    }

    @JsonProperty("primaryrelativerelation")
    public void setPrimaryrelativerelation(Object primaryrelativerelation) {
        this.primaryrelativerelation = primaryrelativerelation;
    }

    @JsonProperty("qualtricsid")
    public String getQualtricsid() {
        return qualtricsid;
    }

    @JsonProperty("qualtricsid")
    public void setQualtricsid(String qualtricsid) {
        this.qualtricsid = qualtricsid;
    }

    @JsonProperty("race")
    public List<String> getRace() {
        return race;
    }

    @JsonProperty("race")
    public void setRace(List<String> race) {
        this.race = race;
    }

    @JsonProperty("seenatclinicalsites")
    public List<String> getSeenatclinicalsites() {
        return seenatclinicalsites;
    }

    @JsonProperty("seenatclinicalsites")
    public void setSeenatclinicalsites(List<String> seenatclinicalsites) {
        this.seenatclinicalsites = seenatclinicalsites;
    }

    @JsonProperty("sequence")
    public List<Sequence> getSequence() {
        return sequence;
    }

    @JsonProperty("sequence")
    public void setSequence(List<Sequence> sequence) {
        this.sequence = sequence;
    }

    @JsonProperty("similarsymptoms")
    public String getSimilarsymptoms() {
        return similarsymptoms;
    }

    @JsonProperty("similarsymptoms")
    public void setSimilarsymptoms(String similarsymptoms) {
        this.similarsymptoms = similarsymptoms;
    }

    @JsonProperty("similarsymptomsexplain")
    public String getSimilarsymptomsexplain() {
        return similarsymptomsexplain;
    }

    @JsonProperty("similarsymptomsexplain")
    public void setSimilarsymptomsexplain(String similarsymptomsexplain) {
        this.similarsymptomsexplain = similarsymptomsexplain;
    }

    @JsonProperty("simpleid")
    public String getSimpleid() {
        return simpleid;
    }

    @JsonProperty("simpleid")
    public void setSimpleid(String simpleid) {
        this.simpleid = simpleid;
    }

    @JsonProperty("symptom")
    public String getSymptom() {
        return symptom;
    }

    @JsonProperty("symptom")
    public void setSymptom(String symptom) {
        this.symptom = symptom;
    }

    @JsonProperty("symptomonset")
    public String getSymptomonset() {
        return symptomonset;
    }

    @JsonProperty("symptomonset")
    public void setSymptomonset(String symptomonset) {
        this.symptomonset = symptomonset;
    }

    @JsonProperty("travellimitations")
    public Object getTravellimitations() {
        return travellimitations;
    }

    @JsonProperty("travellimitations")
    public void setTravellimitations(Object travellimitations) {
        this.travellimitations = travellimitations;
    }

    @JsonProperty("travellimitationsmulti")
    public List<Object> getTravellimitationsmulti() {
        return travellimitationsmulti;
    }

    @JsonProperty("travellimitationsmulti")
    public void setTravellimitationsmulti(List<Object> travellimitationsmulti) {
        this.travellimitationsmulti = travellimitationsmulti;
    }

    @JsonProperty("travellimitationsother")
    public String getTravellimitationsother() {
        return travellimitationsother;
    }

    @JsonProperty("travellimitationsother")
    public void setTravellimitationsother(String travellimitationsother) {
        this.travellimitationsother = travellimitationsother;
    }

    @JsonProperty("udn_referral")
    public String getUdnReferral() {
        return udnReferral;
    }

    @JsonProperty("udn_referral")
    public void setUdnReferral(String udnReferral) {
        this.udnReferral = udnReferral;
    }

    @JsonProperty("udn_referral_method")
    public Object getUdnReferralMethod() {
        return udnReferralMethod;
    }

    @JsonProperty("udn_referral_method")
    public void setUdnReferralMethod(Object udnReferralMethod) {
        this.udnReferralMethod = udnReferralMethod;
    }

    @JsonProperty("uuid")
    public String getUuid() {
        return uuid;
    }

    @JsonProperty("uuid")
    public void setUuid(String uuid) {
        this.uuid = uuid;
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
