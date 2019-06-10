package etl.job.entity.i2b2tm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.opencsv.bean.CsvBindByPosition;


public class PatientMapping {
	private static final Logger logger = LogManager.getLogger(PatientMapping.class);

	@CsvBindByPosition(position = 0)
	private String patientIde;
	@CsvBindByPosition(position = 1)
	private String patientIdeSource;
	@CsvBindByPosition(position = 2)
	private String patientNum;
	@CsvBindByPosition(position = 3)
	private String patientIdeStatus;
	@CsvBindByPosition(position = 4)
	private String projectId;
	@CsvBindByPosition(position = 5)
	private String uploadDate;
	@CsvBindByPosition(position = 6)
	private String downloadDate;
	@CsvBindByPosition(position = 7)
	private String importDate;
	@CsvBindByPosition(position = 8)
	private String sourceSystemCd;
	@CsvBindByPosition(position = 9)
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


	public boolean isValid() {
		return true;
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

	public String getPatientIde() {
		return patientIde;
	}

	public void setPatientIde(String patientIde) {
		this.patientIde = patientIde;
	}

	public String getPatientIdeSource() {
		return patientIdeSource;
	}

	public void setPatientIdeSource(String patientIdeSource) {
		this.patientIdeSource = patientIdeSource;
	}

	public String getPatientNum() {
		return patientNum;
	}

	public void setPatientNum(String patientNum) {
		this.patientNum = patientNum;
	}

	public String getPatientIdeStatus() {
		return patientIdeStatus;
	}

	public void setPatientIdeStatus(String patientIdeStatus) {
		this.patientIdeStatus = patientIdeStatus;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getUploadDate() {
		return uploadDate;
	}

	public void setUploadDate(String uploadDate) {
		this.uploadDate = uploadDate;
	}

	public String getDownloadDate() {
		return downloadDate;
	}

	public void setDownloadDate(String downloadDate) {
		this.downloadDate = downloadDate;
	}

	public String getImportDate() {
		return importDate;
	}

	public void setImportDate(String importDate) {
		this.importDate = importDate;
	}

	public String getSourceSystemCd() {
		return sourceSystemCd;
	}

	public void setSourceSystemCd(String sourceSystemCd) {
		this.sourceSystemCd = sourceSystemCd;
	}

	public String getUploadId() {
		return uploadId;
	}

	public void setUploadId(String uploadId) {
		this.uploadId = uploadId;
	}

	public static Logger getLogger() {
		return logger;
	}
	
	
}
