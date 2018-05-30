
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
    "apgar1",
    "apgar5"
})
public class Apgar {

    @JsonProperty("apgar1")
    private Integer apgar1;
    @JsonProperty("apgar5")
    private Integer apgar5;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("apgar1")
    public Integer getApgar1() {
        return apgar1;
    }

    @JsonProperty("apgar1")
    public void setApgar1(Integer apgar1) {
        this.apgar1 = apgar1;
    }

    @JsonProperty("apgar5")
    public Integer getApgar5() {
        return apgar5;
    }

    @JsonProperty("apgar5")
    public void setApgar5(Integer apgar5) {
        this.apgar5 = apgar5;
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
