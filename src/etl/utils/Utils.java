package etl.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.instrument.Instrumentation;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

public class Utils {

	public static char ESCAPE_CHAR = '\\';
	
	private static volatile Instrumentation globalInstrumentation;
	 
    public static void premain(final String agentArgs, final Instrumentation inst) {
        globalInstrumentation = inst;
    }
 
    public static long getObjectSize(final Object object) {
        if (globalInstrumentation == null) {
            throw new IllegalStateException("Agent not initialized.");
        }
        return globalInstrumentation.getObjectSize(object);
    }	
    
	public static <T> CsvToBean<T> readCsvToBean(Class<T> _class, BufferedReader buffer, char quoteChar, char separator, boolean skipheader) {
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
		StatefulBeanToCsv<T> writer = new StatefulBeanToCsvBuilder<T>(buffer)
				.withQuotechar(quotedString)
				.withSeparator(dataSeparator)
				.withEscapechar(ESCAPE_CHAR)
				.build();
		
		writer.write(objectsToWrite);
	}
	
	public static <T> void writeToCsv(BufferedWriter buffer,Collection<T> objectsToWrite,char quotedString,char dataSeparator) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
		StatefulBeanToCsv<T> writer = new StatefulBeanToCsvBuilder<T>(buffer)
				.withQuotechar(quotedString)
				.withSeparator(dataSeparator)
				.withEscapechar(ESCAPE_CHAR)
				.build();
		
		writer.write(objectsToWrite.stream().collect(Collectors.toList()));
	}

	// checks passed arguments and sends back value for that argument
	public static String checkPassedArgs(String arg, String[] args) throws Exception {
		
		int argcount = 0;
		
		String argv = new String();
		
		for(String thisarg: args) {
			
			if(thisarg.equals(arg)) {
				
				break;
				
			} else {
				
				argcount++;
				
			}
		}
		
		if(args.length > argcount) {
			
			argv = args[argcount + 1];
			
		} else {
			
			throw new Exception("Error in argument: " + arg );
			
		}
		return argv;
	}	
	


}
