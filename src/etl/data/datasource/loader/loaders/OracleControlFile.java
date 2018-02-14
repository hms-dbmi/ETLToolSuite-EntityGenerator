package etl.data.datasource.loader.loaders;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OracleControlFile {
	private String inFile;
	private String outFile;
	private String badFile;
	private String discardFile;
	private boolean isAppend;
	private String table;
	private String whenClause;
	private boolean trailingNullCols;
	private List<String> fieldList = new ArrayList<String>();
	
	public OracleControlFile() {
		super();
	}

	public String getInFile() {
		return inFile;
	}

	public void setInFile(String inFile) {
		this.inFile = inFile;
	}

	public String getBadFile() {
		return badFile;
	}

	public void setBadFile(String badFile) {
		this.badFile = badFile;
	}

	public String getDiscardFile() {
		return discardFile;
	}

	public void setDiscardFile(String discardFile) {
		this.discardFile = discardFile;
	}

	public boolean isAppend() {
		return isAppend;
	}

	public void setAppend(boolean isAppend) {
		this.isAppend = isAppend;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getWhenClause() {
		return whenClause;
	}

	public void setWhenClause(String whenClause) {
		this.whenClause = whenClause;
	}

	public boolean isTrailingNullCols() {
		return trailingNullCols;
	}

	public void setTrailingNullCols(boolean trailingNullCols) {
		this.trailingNullCols = trailingNullCols;
	}

	public List<String> getFieldList() {
		return fieldList;
	}

	public void setFieldList(List<String> fieldList) {
		this.fieldList = fieldList;
	}

	@Override
	public String toString() {
		String string = "LOAD DATA \n" +
			"INFILE '" + inFile + "'\n" +
			"OUTFILE '" + outFile + "'\n" +
			"BADFILE '" + badFile + "'\n" +
			"DISCARDFILE '" + discardFile  + "'\n" +
			"APPEND \n" +
			"INTO TABLE " + table + "\n" +
			"( \n";
		
		Iterator<String> iter = fieldList.iterator();
		
		while(iter.hasNext()){
			String next = iter.next();
			if(iter.hasNext()){
				string += next + ",\n";
			} else {
				string += next + "\n)";
			}
		}
			
		return string;		
	}
	
	
}
