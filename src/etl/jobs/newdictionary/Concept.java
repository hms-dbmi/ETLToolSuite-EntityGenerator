package etl.jobs.newdictionary;

import com.fasterxml.jackson.databind.JsonNode;

public class Concept {
    String dataset= "";
    String conceptName="";
    String displayName="";
    String conceptType="";
    String conceptPath="";
    String parentConceptPath="";
    String values="";
    String description="";
    Boolean stigmatized=false;


    public Concept() {
    }   

    public String getDataset() {
        return this.dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getConceptName() {
        return this.conceptName;
    }

    public void setConceptName(String conceptName) {
        this.conceptName = conceptName;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getConceptType() {
        return this.conceptType;
    }

    public void setConceptType(String conceptType) {
        this.conceptType = conceptType;
    }

    public String getConceptPath() {
        return this.conceptPath;
    }

    public void setConceptPath(String conceptPath) {
        this.conceptPath = conceptPath;
    }

    public String getParentConceptPath() {
        return this.parentConceptPath;
    }

    public void setParentConceptPath(String parentConceptPath) {
        this.parentConceptPath = parentConceptPath;
    }

    public String getValues() {
        return this.values;
    }

    public void setValues(String values) {
        this.values = values;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean isStigmatized() {
        return this.stigmatized;
    }

    public Boolean getStigmatized() {
        return this.stigmatized;
    }

    public void setStigmatized(Boolean stigmatized) {
        this.stigmatized = stigmatized;
    }

   
    public String[] getCsvEntry() {
        String[] entry = {getDataset(),getConceptName(),getDisplayName(), getConceptType(), getConceptPath(),getParentConceptPath(), getValues() ,getDescription(), isStigmatized().toString()};
        return entry;
    }


}
