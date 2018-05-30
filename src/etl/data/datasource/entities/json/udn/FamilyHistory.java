
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
    "affectedRelatives",
    "consanguinity",
    "miscarriages"
})
public class FamilyHistory {

    @JsonProperty("affectedRelatives")
    private Boolean affectedRelatives;
    @JsonProperty("consanguinity")
    private Boolean consanguinity;
    @JsonProperty("miscarriages")
    private Boolean miscarriages;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("affectedRelatives")
    public Boolean getAffectedRelatives() {
        return affectedRelatives;
    }

    @JsonProperty("affectedRelatives")
    public void setAffectedRelatives(Boolean affectedRelatives) {
        this.affectedRelatives = affectedRelatives;
    }

    @JsonProperty("consanguinity")
    public Boolean getConsanguinity() {
        return consanguinity;
    }

    @JsonProperty("consanguinity")
    public void setConsanguinity(Boolean consanguinity) {
        this.consanguinity = consanguinity;
    }

    @JsonProperty("miscarriages")
    public Boolean getMiscarriages() {
        return miscarriages;
    }

    @JsonProperty("miscarriages")
    public void setMiscarriages(Boolean miscarriages) {
        this.miscarriages = miscarriages;
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
