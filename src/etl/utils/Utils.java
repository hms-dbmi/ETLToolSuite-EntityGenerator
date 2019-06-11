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

import etl.job.entity.Mapping;
import etl.job.entity.i2b2tm.ConceptDimension;
import etl.job.entity.i2b2tm.ObservationFact;

public class Utils {

	private static char ESCAPE_CHAR = '≈';
	
	public static <T> CsvToBean<T> readCsvToBean(Class<T> _class, BufferedReader buffer, char quoteChar, char separator, boolean skipheader) {
		@SuppressWarnings("unchecked")
		CsvToBean<T> beans = new CsvToBeanBuilder<T>(buffer)
				.withSkipLines(skipheader ? 1 : 0)
				.withQuoteChar(quoteChar)
				.withSeparator(separator)
				.withEscapeChar(ESCAPE_CHAR)
				.withType(_class)
				.build();
		return beans;
		
	}
	public static <T> void writeToCsv(BufferedWriter buffer,List<T> objectsToWrite,char quotedString,char dataSeparator) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		@SuppressWarnings("unchecked")
		StatefulBeanToCsv<T> writer = new StatefulBeanToCsvBuilder(buffer)
				.withQuotechar(quotedString)
				.withSeparator(dataSeparator)
				.withEscapechar(ESCAPE_CHAR)
				.build();
		
		writer.write(objectsToWrite);
	}
}
