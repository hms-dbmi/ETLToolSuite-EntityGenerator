
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
    "assistedReproduction_donoregg",
    "assistedReproduction_donorsperm",
    "assistedReproduction_fertilityMeds",
    "assistedReproduction_iui",
    "assistedReproduction_surrogacy",
    "gestation",
    "icsi",
    "ivf",
    "maternal_age",
    "multipleGestation",
    "obstetric-history",
    "paternal_age",
    "twinNumber"
})
public class PrenatalPerinatalHistory {

    @JsonProperty("assistedReproduction_donoregg")
    private Boolean assistedReproductionDonoregg;
    @JsonProperty("assistedReproduction_donorsperm")
    private Boolean assistedReproductionDonorsperm;
    @JsonProperty("assistedReproduction_fertilityMeds")
    private Boolean assistedReproductionFertilityMeds;
    @JsonProperty("assistedReproduction_iui")
    private Boolean assistedReproductionIui;
    @JsonProperty("assistedReproduction_surrogacy")
    private Boolean assistedReproductionSurrogacy;
    @JsonProperty("gestation")
    private Integer gestation;
    @JsonProperty("icsi")
    private Boolean icsi;
    @JsonProperty("ivf")
    private Boolean ivf;
    @JsonProperty("maternal_age")
    private Integer maternalAge;
    @JsonProperty("multipleGestation")
    private Boolean multipleGestation;
    @JsonProperty("obstetric-history")
    private ObstetricHistory obstetricHistory;
    @JsonProperty("paternal_age")
    private Integer paternalAge;
    @JsonProperty("twinNumber")
    private Object twinNumber;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("assistedReproduction_donoregg")
    public Boolean getAssistedReproductionDonoregg() {
        return assistedReproductionDonoregg;
    }

    @JsonProperty("assistedReproduction_donoregg")
    public void setAssistedReproductionDonoregg(Boolean assistedReproductionDonoregg) {
        this.assistedReproductionDonoregg = assistedReproductionDonoregg;
    }

    @JsonProperty("assistedReproduction_donorsperm")
    public Boolean getAssistedReproductionDonorsperm() {
        return assistedReproductionDonorsperm;
    }

    @JsonProperty("assistedReproduction_donorsperm")
    public void setAssistedReproductionDonorsperm(Boolean assistedReproductionDonorsperm) {
        this.assistedReproductionDonorsperm = assistedReproductionDonorsperm;
    }

    @JsonProperty("assistedReproduction_fertilityMeds")
    public Boolean getAssistedReproductionFertilityMeds() {
        return assistedReproductionFertilityMeds;
    }

    @JsonProperty("assistedReproduction_fertilityMeds")
    public void setAssistedReproductionFertilityMeds(Boolean assistedReproductionFertilityMeds) {
        this.assistedReproductionFertilityMeds = assistedReproductionFertilityMeds;
    }

    @JsonProperty("assistedReproduction_iui")
    public Boolean getAssistedReproductionIui() {
        return assistedReproductionIui;
    }

    @JsonProperty("assistedReproduction_iui")
    public void setAssistedReproductionIui(Boolean assistedReproductionIui) {
        this.assistedReproductionIui = assistedReproductionIui;
    }

    @JsonProperty("assistedReproduction_surrogacy")
    public Boolean getAssistedReproductionSurrogacy() {
        return assistedReproductionSurrogacy;
    }

    @JsonProperty("assistedReproduction_surrogacy")
    public void setAssistedReproductionSurrogacy(Boolean assistedReproductionSurrogacy) {
        this.assistedReproductionSurrogacy = assistedReproductionSurrogacy;
    }

    @JsonProperty("gestation")
    public Integer getGestation() {
        return gestation;
    }

    @JsonProperty("gestation")
    public void setGestation(Integer gestation) {
        this.gestation = gestation;
    }

    @JsonProperty("icsi")
    public Boolean getIcsi() {
        return icsi;
    }

    @JsonProperty("icsi")
    public void setIcsi(Boolean icsi) {
        this.icsi = icsi;
    }

    @JsonProperty("ivf")
    public Boolean getIvf() {
        return ivf;
    }

    @JsonProperty("ivf")
    public void setIvf(Boolean ivf) {
        this.ivf = ivf;
    }

    @JsonProperty("maternal_age")
    public Integer getMaternalAge() {
        return maternalAge;
    }

    @JsonProperty("maternal_age")
    public void setMaternalAge(Integer maternalAge) {
        this.maternalAge = maternalAge;
    }

    @JsonProperty("multipleGestation")
    public Boolean getMultipleGestation() {
        return multipleGestation;
    }

    @JsonProperty("multipleGestation")
    public void setMultipleGestation(Boolean multipleGestation) {
        this.multipleGestation = multipleGestation;
    }

    @JsonProperty("obstetric-history")
    public ObstetricHistory getObstetricHistory() {
        return obstetricHistory;
    }

    @JsonProperty("obstetric-history")
    public void setObstetricHistory(ObstetricHistory obstetricHistory) {
        this.obstetricHistory = obstetricHistory;
    }

    @JsonProperty("paternal_age")
    public Integer getPaternalAge() {
        return paternalAge;
    }

    @JsonProperty("paternal_age")
    public void setPaternalAge(Integer paternalAge) {
        this.paternalAge = paternalAge;
    }

    @JsonProperty("twinNumber")
    public Object getTwinNumber() {
        return twinNumber;
    }

    @JsonProperty("twinNumber")
    public void setTwinNumber(Object twinNumber) {
        this.twinNumber = twinNumber;
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
