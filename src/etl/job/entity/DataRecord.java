package etl.job.entity;

public class DataRecord {
	private int rowIndex;
	private String value;
	
	public DataRecord(int rowIndex, int colIndex, String value) {
		super();
		this.rowIndex = rowIndex;
		this.value = value;
	}
	public int getRowIndex() {
		return rowIndex;
	}
	public void setRowIndex(int rowIndex) {
		this.rowIndex = rowIndex;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "DataRecord [rowIndex=" + rowIndex + ", value=" + value + "]";
	}
	
	
}
