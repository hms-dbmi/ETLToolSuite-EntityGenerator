package etl.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.List;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

public class Utils {
	
	public static <T> CsvToBean<T> readCsv(BufferedReader buffer, char quoteChar, char separator) {
		return new CsvToBeanBuilder<T>(buffer)
				.withQuoteChar(quoteChar)
				.withSeparator(separator)
				.build();
	}
	
	public static <T> void writeToCsv(BufferedWriter buffer,List<T> objectsToWrite,char quotedString,char dataSeparator) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		StatefulBeanToCsv<T> writer = new StatefulBeanToCsvBuilder(buffer)
				.withQuotechar(quotedString)
				.withSeparator(dataSeparator)
				.build();
		
		writer.write(objectsToWrite);
	}
}
