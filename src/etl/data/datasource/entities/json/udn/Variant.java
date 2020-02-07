
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
    "cdna",
    "effect",
    "evidence",
    "gene",
    "inheritance",
    "interpretation",
    "sanger",
    "segregation",
    "zygosity"
})
public class Variant {

    @JsonProperty("cdna")
    private String cdna;
    @JsonProperty("effect")
    private String effect;
    @JsonProperty("evidence")
    private List<String> evidence = null;
    @JsonProperty("gene")
    private String gene;
    @JsonProperty("inheritance")
    private String inheritance;
    @JsonProperty("interpretation")
    private String interpretation;
    @JsonProperty("sanger")
    private String sanger;
    @JsonProperty("segregation")
    private String segregation;
    @JsonProperty("zygosity")
    private String zygosity;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("cdna")
    public String getCdna() {
        return cdna;
    }

    @JsonProperty("cdna")
    public void setCdna(String cdna) {
        this.cdna = cdna;
    }

    @JsonProperty("effect")
    public String getEffect() {
        return effect;
    }

    @JsonProperty("effect")
    public void setEffect(String effect) {
        this.effect = effect;
    }

    @JsonProperty("evidence")
    public List<String> getEvidence() {
        return evidence;
    }

    @JsonProperty("evidence")
    public void setEvidence(List<String> evidence) {
        this.evidence = evidence;
    }

    @JsonProperty("gene")
    public String getGene() {
        return gene;
    }

    @JsonProperty("gene")
    public void setGene(String gene) {
        this.gene = gene;
    }

    @JsonProperty("inheritance")
    public String getInheritance() {
        return inheritance;
    }

    @JsonProperty("inheritance")
    public void setInheritance(String inheritance) {
        this.inheritance = inheritance;
    }

    @JsonProperty("interpretation")
    public String getInterpretation() {
        return interpretation;
    }

    @JsonProperty("interpretation")
    public void setInterpretation(String interpretation) {
        this.interpretation = interpretation;
    }

    @JsonProperty("sanger")
    public String getSanger() {
        return sanger;
    }

    @JsonProperty("sanger")
    public void setSanger(String sanger) {
        this.sanger = sanger;
    }

    @JsonProperty("segregation")
    public String getSegregation() {
        return segregation;
    }

    @JsonProperty("segregation")
    public void setSegregation(String segregation) {
        this.segregation = segregation;
    }

    @JsonProperty("zygosity")
    public String getZygosity() {
        return zygosity;
    }

    @JsonProperty("zygosity")
    public void setZygosity(String zygosity) {
        this.zygosity = zygosity;
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
