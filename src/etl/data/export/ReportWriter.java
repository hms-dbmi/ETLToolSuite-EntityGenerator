package etl.data.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import etl.data.export.entities.Entity;

public class ReportWriter {
	public static String WRITE_PROCESSING_DIR = "";
	
	public static String WRITE_COMPLETED_DIR = ""; 
	
	public void writeToFile(String text, String fileName) {	
		BufferedWriter writer = null;
		// prep buffers.  Need to keep buffers open.  Placing them in a map and handling them as below keeps them open and will
		// close all buffers in the map in the finally clause. Probably a bit overkill as stream should close them.
		try{
				
				
					String destPath = WRITE_PROCESSING_DIR + fileName + ".tsv";
					
					if(!Files.exists(Paths.get(destPath))){
						
						File file = new File(destPath);
						
						file.createNewFile();
						
					}	
					
					writer = Files.newBufferedWriter(Paths.get(destPath), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
					
					writer.write(text);
					writer.flush();

			} catch (Exception e) {
				
			} finally {
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

	}
}
