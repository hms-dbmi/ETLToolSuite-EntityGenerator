package etl.data.datasource;

import java.io.FileNotFoundException;
import java.io.FileReader;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;

public class CSVDataSource extends DataSource {

	public CSVDataSource(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}
	
	public au.com.bytecode.opencsv.CSVReader processCSV(String csvFile, char delimiter, int skipHeader) throws FileNotFoundException{
		
		long startTime = System.currentTimeMillis();
		//CSVParser parser = new CSVParser(delimiter,'"','\0',CSVParser.DEFAULT_STRICT_QUOTES);
		
		au.com.bytecode.opencsv.CSVReader csvreader = new CSVReader(new FileReader(csvFile), delimiter, '"','\0', skipHeader);
		
		//au.com.bytecode.opencsv.CSVReader csvreader = new CSVReader (new FileReader(csvFile), 0, parser);
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		//System.out.println("ProcessCSV Time: " + elapsedTime);

		return csvreader;
	}

	@Override
	/**
	 * String 0 = Csvfile needed to process
	 * String 1 if given is delimiter...default is ','
	 */
	public Object processData(String ... args) throws FileNotFoundException {
		
		String csvFile = args[0];
		
		char delimiter = ',';
		
		if(args.length > 1){
			
			delimiter = args[1].toCharArray()[0];
			
		}
		
		long startTime = System.currentTimeMillis();
		//CSVParser parser = new CSVParser(delimiter,'"','\0',CSVParser.DEFAULT_STRICT_QUOTES);
		
		au.com.bytecode.opencsv.CSVReader csvreader = new CSVReader(new FileReader(csvFile), delimiter, '"','\0', 1);
		
		//au.com.bytecode.opencsv.CSVReader csvreader = new CSVReader (new FileReader(csvFile), 0, parser);
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		//System.out.println("ProcessCSV Time: " + elapsedTime);

		return csvreader;
	}
	
}
