
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
    "maternal_ethnicity",
    "paternal_ethnicity"
})
public class Ethnicity {

    @JsonProperty("maternal_ethnicity")
    private List<String> maternalEthnicity = null;
    @JsonProperty("paternal_ethnicity")
    private List<String> paternalEthnicity = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("maternal_ethnicity")
    public List<String> getMaternalEthnicity() {
        return maternalEthnicity;
    }

    @JsonProperty("maternal_ethnicity")
    public void setMaternalEthnicity(List<String> maternalEthnicity) {
        this.maternalEthnicity = maternalEthnicity;
    }

    @JsonProperty("paternal_ethnicity")
    public List<String> getPaternalEthnicity() {
        return paternalEthnicity;
    }

    @JsonProperty("paternal_ethnicity")
    public void setPaternalEthnicity(List<String> paternalEthnicity) {
        this.paternalEthnicity = paternalEthnicity;
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
