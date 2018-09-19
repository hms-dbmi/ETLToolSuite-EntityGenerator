package etl.data.export.entities.i2b2;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.utils.ColumnSequencer;
import etl.mapping.CsvToI2b2TMMapping;

public class PatientMapping extends Entity {
	private static final Logger logger = LogManager.getLogger(PatientMapping.class);

	private String patientIde;
	private String patientIdeSource;
	private String patientNum;
	private String patientIdeStatus;
	private String projectId;
	private String uploadDate;
	private String downloadDate;
	private String importDate;
	private String sourceSystemCd;
	private String uploadId;
	
	public PatientMapping(String str, String patientIde, String patientIdeSource, String patientNum,
			String patientIdeStatus, String projectId, String uploadDate, String downloadDate, String importDate,
			String sourceSystemCd, String uploadId) throws Exception {
		this.patientIde = patientIde;
		this.patientIdeSource = patientIdeSource;
		this.patientNum = patientNum;
		this.patientIdeStatus = patientIdeStatus;
		this.projectId = projectId;
		this.uploadDate = uploadDate;
		this.downloadDate = downloadDate;
		this.importDate = importDate;
		this.sourceSystemCd = sourceSystemCd;
		this.uploadId = uploadId;
	}
	
	public PatientMapping(String patientIde, String patientIdeSource, String patientNum,
			String sourceSystemCd) {
		this.patientIde = patientIde;
		this.patientIdeSource = patientIdeSource;
		this.patientNum = patientNum;
		this.sourceSystemCd = sourceSystemCd;
	}

	@Override
	public boolean isValid() {
		return true;
	}

	@Override
	public String toCsv() {
		return makeStringSafe(this.patientIde) + "," + makeStringSafe(this.patientIdeSource) + "," 
				+ makeStringSafe(this.patientNum) + "," + makeStringSafe(this.sourceSystemCd);
	}
	
	
	public static Set<PatientMapping> objectMappingToPatientMapping(Set<ObjectMapping> objectMappings) throws Exception{
		Set<PatientMapping> pms = new HashSet<PatientMapping>();
		
		for(ObjectMapping om: objectMappings) {
			if(om.getSourceName().substring(0,2).equalsIgnoreCase("ID")) {
				pms.add(new PatientMapping(om.getSourceId(),om.getSourceName(),om.getMappedId(),Entity.SOURCESYSTEM_CD));
			}
		}
		
		
		return pms;
		
	}

	@Override
	public void buildEntity(String str, CsvToI2b2TMMapping mapping, String[] data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patientIde == null) ? 0 : patientIde.hashCode());
		result = prime * result + ((patientIdeSource == null) ? 0 : patientIdeSource.hashCode());
		result = prime * result + ((patientNum == null) ? 0 : patientNum.hashCode());
		result = prime * result + ((projectId == null) ? 0 : projectId.hashCode());
		result = prime * result + ((sourceSystemCd == null) ? 0 : sourceSystemCd.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PatientMapping other = (PatientMapping) obj;
		if (patientIde == null) {
			if (other.patientIde != null)
				return false;
		} else if (!patientIde.equals(other.patientIde))
			return false;
		if (patientIdeSource == null) {
			if (other.patientIdeSource != null)
				return false;
		} else if (!patientIdeSource.equals(other.patientIdeSource))
			return false;
		if (patientNum == null) {
			if (other.patientNum != null)
				return false;
		} else if (!patientNum.equals(other.patientNum))
			return false;
		
		return true;
	}
	
	
}
