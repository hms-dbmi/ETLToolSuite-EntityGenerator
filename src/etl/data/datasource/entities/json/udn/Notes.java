
package etl.data.datasource.entities.json.udn;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "diagnosis_notes",
    "family_history",
    "genetic_notes",
    "indication_for_referral",
    "medical_history",
    "prenatal_development"
})
public class Notes {

    @JsonProperty("diagnosis_notes")
    private String diagnosisNotes;
    @JsonProperty("family_history")
    private String familyHistory;
    @JsonProperty("genetic_notes")
    private String geneticNotes;
    @JsonProperty("indication_for_referral")
    private String indicationForReferral;
    @JsonProperty("medical_history")
    private String medicalHistory;
    @JsonProperty("prenatal_development")
    private String prenatalDevelopment;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("diagnosis_notes")
    public String getDiagnosisNotes() {
        return diagnosisNotes;
    }

    @JsonProperty("diagnosis_notes")
    public void setDiagnosisNotes(String diagnosisNotes) {
        this.diagnosisNotes = diagnosisNotes;
    }

    @JsonProperty("family_history")
    public String getFamilyHistory() {
        return familyHistory;
    }

    @JsonProperty("family_history")
    public void setFamilyHistory(String familyHistory) {
        this.familyHistory = familyHistory;
    }

    @JsonProperty("genetic_notes")
    public String getGeneticNotes() {
        return geneticNotes;
    }

    @JsonProperty("genetic_notes")
    public void setGeneticNotes(String geneticNotes) {
        this.geneticNotes = geneticNotes;
    }

    @JsonProperty("indication_for_referral")
    public String getIndicationForReferral() {
        return indicationForReferral;
    }

    @JsonProperty("indication_for_referral")
    public void setIndicationForReferral(String indicationForReferral) {
        this.indicationForReferral = indicationForReferral;
    }

    @JsonProperty("medical_history")
    public String getMedicalHistory() {
        return medicalHistory;
    }

    @JsonProperty("medical_history")
    public void setMedicalHistory(String medicalHistory) {
        this.medicalHistory = medicalHistory;
    }

    @JsonProperty("prenatal_development")
    public String getPrenatalDevelopment() {
        return prenatalDevelopment;
    }

    @JsonProperty("prenatal_development")
    public void setPrenatalDevelopment(String prenatalDevelopment) {
        this.prenatalDevelopment = prenatalDevelopment;
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
