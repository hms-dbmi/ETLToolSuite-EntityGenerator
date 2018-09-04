package etl.data.datasource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CSVDataSource3 extends DataSource {
	public static char DELIMITER = ',';
	public static char QUOTE_CHAR = '"';
	public static char ESCAPE_CHAR = '\0';
	public static int SKIP_HEADER = 0;
	
	public CSVDataSource3(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	public static com.opencsv.CSVReader processCSV(String csvFile) throws IOException{
					
        Reader reader = Files.newBufferedReader(Paths.get(csvFile));
        com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(reader,'\t','"');
		return csvReader;
		
	}

	@Override
	public List processData(String... arguments) throws IOException {
		String fileName = arguments[0];
		List list = new ArrayList<>();
		Path path = Paths.get(fileName);
		
		if(!Files.isDirectory(path)) {
			com.opencsv.CSVReader reader = processCSV(fileName);
			
			String[] arr = reader.readNext();
			int recs = 1;
			while(arr != null) {

				Map map = new LinkedHashMap<>();
				Integer col = 0;	
				
				for(String str:arr) {
					map.put(path.getFileName() + ":" + col.toString(), Arrays.asList(str));
					col++;
				}
				recs++;
				list.add(map);
				arr = reader.readNext();
			}		
			
		} else {
			// todo dir
		}
		
		
		return list;
	}

}
