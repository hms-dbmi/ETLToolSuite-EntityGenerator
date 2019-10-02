package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.I2B2;
import etl.utils.Utils;

public class FixPaths extends Job {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5199112745478580367L;

	private static Map<String, String> conceptcdLookup = new HashMap<String, String>();

	
	public static void main(String[] args) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){

			CsvToBean<ConceptDimension> csvToBean = Utils.readCsvToBean(ConceptDimension.class, reader, DATA_QUOTED_STRING, DATA_SEPARATOR, false);	
			csvToBean.spliterator().forEachRemaining(cd -> {
				if(conceptcdLookup.containsKey(cd.getConceptPath())) {
					//System.out.println(cd);
				} else {
					conceptcdLookup.put(cd.getConceptPath(), cd.getConceptCd());
				}
			});
			//conceptcdLookup = StreamSupport.stream(csvToBean.spliterator(), false).collect(Collectors.toMap(ConceptDimension::getConceptPath, ConceptDimension::getConceptCd));
			
		}
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){
			
			RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
			
			CSVReader csvReader = new CSVReaderBuilder(reader)
			                .withCSVParser(rfc4180Parser).build();
			
			List<String[]> records = new ArrayList<String[]>();

			csvReader.forEach(record ->{
				I2B2 i2b2 = new I2B2(record);
				
				String path = StringUtils.replace(i2b2.getcFullName(), PATH_SEPARATOR.toString(), "\\");
				
				String cBasecode  = conceptcdLookup.containsKey(i2b2.getcFullName()) ? conceptcdLookup.get(i2b2.getcFullName()):
					"";
					
				
				i2b2.setcDimCode(path);
				i2b2.setcFullName(path);
				i2b2.setcToolTip(path);
				i2b2.setcBaseCode(cBasecode);
				if(!i2b2.getcMetaDataXML().isEmpty()) i2b2.setcMetaDataXML("NUMERIC");
				//records.add(i2b2.toStringArray());

				try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "I2B2new.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){				
					//CsvToBean<I2B2> csvToBean = 
					//		Utils.readCsvToBean(I2B2.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
					CSVWriter writer = new CSVWriter(buffer);
			
					writer.writeNext(i2b2.toStringArray());
					
					writer.flush();

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			
			csvReader.close();
			
			reader.close();
			

			
		}
		
		try(BufferedReader reader = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimension.csv"))){

			RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
			
			CSVReader csvReader = new CSVReaderBuilder(reader)
			                .withCSVParser(rfc4180Parser).build();
			
			//List<String[]> records = new ArrayList<String[]>();
			
			csvReader.forEach(record ->{
				
				String path = StringUtils.replace(record[1], PATH_SEPARATOR.toString(), "\\");
				
				record[1] = path;
				
				//records.add(record);
				
				try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "ConceptDimensionnew.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
					
					CSVWriter writer = new CSVWriter(buffer);
					
					writer.writeNext(record);
					
					
					writer.flush();
					

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			});
			csvReader.close();

			reader.close();
						
		}
	}

}
