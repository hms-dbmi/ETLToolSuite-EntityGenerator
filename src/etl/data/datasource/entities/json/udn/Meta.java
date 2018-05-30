
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
    "chebi_version",
    "hgncRemote_version",
    "hgnc_version",
    "hpo_version",
    "omim_version",
    "phenotips_version"
})
public class Meta {

    @JsonProperty("chebi_version")
    private String chebiVersion;
    @JsonProperty("hgncRemote_version")
    private String hgncRemoteVersion;
    @JsonProperty("hgnc_version")
    private String hgncVersion;
    @JsonProperty("hpo_version")
    private String hpoVersion;
    @JsonProperty("omim_version")
    private String omimVersion;
    @JsonProperty("phenotips_version")
    private String phenotipsVersion;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("chebi_version")
    public String getChebiVersion() {
        return chebiVersion;
    }

    @JsonProperty("chebi_version")
    public void setChebiVersion(String chebiVersion) {
        this.chebiVersion = chebiVersion;
    }

    @JsonProperty("hgncRemote_version")
    public String getHgncRemoteVersion() {
        return hgncRemoteVersion;
    }

    @JsonProperty("hgncRemote_version")
    public void setHgncRemoteVersion(String hgncRemoteVersion) {
        this.hgncRemoteVersion = hgncRemoteVersion;
    }

    @JsonProperty("hgnc_version")
    public String getHgncVersion() {
        return hgncVersion;
    }

    @JsonProperty("hgnc_version")
    public void setHgncVersion(String hgncVersion) {
        this.hgncVersion = hgncVersion;
    }

    @JsonProperty("hpo_version")
    public String getHpoVersion() {
        return hpoVersion;
    }

    @JsonProperty("hpo_version")
    public void setHpoVersion(String hpoVersion) {
        this.hpoVersion = hpoVersion;
    }

    @JsonProperty("omim_version")
    public String getOmimVersion() {
        return omimVersion;
    }

    @JsonProperty("omim_version")
    public void setOmimVersion(String omimVersion) {
        this.omimVersion = omimVersion;
    }

    @JsonProperty("phenotips_version")
    public String getPhenotipsVersion() {
        return phenotipsVersion;
    }

    @JsonProperty("phenotips_version")
    public void setPhenotipsVersion(String phenotipsVersion) {
        this.phenotipsVersion = phenotipsVersion;
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
