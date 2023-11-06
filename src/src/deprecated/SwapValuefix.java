package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.opencsv.CSVReader;

public class SwapValuefix {

	public static void main(String[] args) throws IOException {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("/home/ec2-user/pic-sure-hpds/docker/pic-sure-hpds-etl/hpds/allConcepts.csv"),StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.CREATE)){
			try(BufferedReader buffer = Files.newBufferedReader(Paths.get("/home/ec2-user/pic-sure-hpds/docker/pic-sure-hpds-etl/allConcepts.csv"))){
				
				CSVReader reader = new CSVReader(buffer, ',', '\"', '√');
				
				String[] line;
				
				while((line = reader.readNext()) != null) {
					
					String[] newLine = new String[line.length];
					newLine[0] = line[0];
					newLine[1] = line[1];
					newLine[2] = line[3];
					newLine[3] = line[2];
					
					writer.write(toCsv(newLine));
				}
				
			}
			writer.flush();
			writer.close();
		}
	}
	
	private static char[] toCsv(String[] strings) {
		StringBuilder sb = new StringBuilder();
		
		int x = strings.length;
		
		for(String str: strings) {
			sb.append('"');
			sb.append(str);
			sb.append('"');
			if(x != 1 ) {
				sb.append(',');
			}
			x--;
		}
		sb.append('\n');
		return sb.toString().toCharArray();
	}
}
