package src.deprecated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.I2B2Secure;
import etl.job.entity.i2b2tm.TableAccess;
import etl.jobs.Job;
import etl.utils.Utils;



/**
 * @author Thomas DeSain
 * Take metadata generated from metadatagenerator and replicates 
 * parent nodes to fill out metadata records.
 *
 */
public class FillInTree extends Job{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1357491618995356551L;

	private static boolean SKIP_HEADERS = false;

	private static int clevel = 1; 
	
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
	
	private static void execute() throws Exception {
		Set<I2B2> nodes = new HashSet<I2B2>();
		Set<TableAccess> tableAccess = new HashSet<TableAccess>();

		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){
			
			CsvToBean<I2B2> csvToBean = 
					Utils.readCsvToBean(I2B2.class, buffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
			
			
			
			fillTree(csvToBean);
			
			// build tableaccess entities
			//tableAccess.addAll(TableAccess.createTableAccess(nodes));
		}
	
		//try(BufferedWriter buffer = Files.newBufferedWriter(
		//		Paths.get(WRITE_DIR + File.separatorChar + "TableAccess.csv"))){

		//	Utils.writeToCsv(buffer, tableAccess, DATA_QUOTED_STRING, DATA_SEPARATOR);
		//} 		
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
	public static void fillTree(CsvToBean<I2B2> csvToBean) throws Exception{
		
		Set<String> builtNodes = new HashSet<String>();
		
		//Map<String, String> mapforparallel = new ConcurrentHashMap<String,String>();
		
		//Spliterator<I2B2> spliterator = csvToBean.spliterator().trySplit();
		
		csvToBean.forEach(node ->{
			try(BufferedWriter buffer = Files.newBufferedWriter(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
				Integer x = StringUtils.countMatches(node.getcFullName(), PATH_SEPARATOR) - 1;
				
				Set<I2B2> set = new HashSet<I2B2>();
				
				while(x > 1){
					I2B2 i2b2 = new I2B2(node);
					String newPath = node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), PATH_SEPARATOR, x) + 1);

					if(!builtNodes.contains(newPath)) {
					
						i2b2.setcFullName(newPath);
						
						i2b2.setcDimCode(newPath);
						
						i2b2.setcToolTip(newPath);
						
						i2b2.setcHlevel(new Integer(x - 2).toString());
						i2b2.setcBaseCode(null);
						i2b2.setcVisualAttributes("FA");
						
						i2b2.setcMetaDataXML("");
						
						String[] fullNodes = i2b2.getcFullName().split(PATH_SEPARATOR.toString());
						
						i2b2.setcName(fullNodes[fullNodes.length - 1]);
		
						set.add(i2b2);
						builtNodes.add(newPath);
						
					}
					x--;

				}
				Utils.writeToCsv(buffer, set, DATA_QUOTED_STRING, DATA_SEPARATOR);
				buffer.flush();
			} catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			};			
			
		});
		
		/*spliterator.forEachRemaining(node -> {
			Integer x = StringUtils.countMatches(node.getcFullName(), PATH_SEPARATOR) - 1;
			
			while(x > 1){
				String n = node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), PATH_SEPARATOR, x) + 1);
				mapforparallel.put(n,"");
				x--;
			}
		});
		
		System.out.println("Finished Fetching Nodes.");
		
		System.out.println("Filling in nodes");
		
		BufferedReader rbuffer = Files.newBufferedReader(Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"));
		
		csvToBean = 
				Utils.readCsvToBean(I2B2.class, rbuffer, DATA_QUOTED_STRING, DATA_SEPARATOR, SKIP_HEADERS);
		
		csvToBean.forEach(node ->{
			ArrayList<I2B2> set = new ArrayList<I2B2>();
			ArrayList<I2B2Secure> secure = new ArrayList<I2B2Secure>();
			Integer x = StringUtils.countMatches(node.getcFullName(), PATH_SEPARATOR) - 1;
			while(x > 1){
				I2B2 i2b2 = new I2B2(node);
				
				String newPath = node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), PATH_SEPARATOR, x) + 1);

				if(!mapforparallel.containsKey(newPath)) {
					break;
				} 
				i2b2.setcFullName(newPath);
				
				i2b2.setcDimCode(newPath);
				
				i2b2.setcToolTip(newPath);
				
				i2b2.setcHlevel(new Integer(x - 2).toString());
				i2b2.setcBaseCode(null);
				i2b2.setcVisualAttributes("FA");
				
				i2b2.setcMetaDataXML("");
				
				String[] fullNodes = i2b2.getcFullName().split(PATH_SEPARATOR.toString());
				
				i2b2.setcName(fullNodes[fullNodes.length - 1]);
				

				set.add(i2b2);
				secure.add(new I2B2Secure(i2b2));
				
				if(mapforparallel.containsKey(newPath)) {
					mapforparallel.remove(newPath);
				}
					//System.out.println(nodesToBuild.size());
				//}
				
				x--;
			}

			try(BufferedWriter buffer = Files.newBufferedWriter(
					Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
				
				try {

					Utils.writeToCsv(buffer, set, DATA_QUOTED_STRING, DATA_SEPARATOR);
					buffer.flush();
					
				} catch (CsvDataTypeMismatchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (CsvRequiredFieldEmptyException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try(BufferedWriter buffer = Files.newBufferedWriter(
					Paths.get(WRITE_DIR + File.separatorChar + "I2B2Secure.csv"),StandardOpenOption.CREATE, StandardOpenOption.APPEND)){
				
				try {

					Utils.writeToCsv(buffer, secure, DATA_QUOTED_STRING, DATA_SEPARATOR);
					buffer.flush();
					
				} catch (CsvDataTypeMismatchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (CsvRequiredFieldEmptyException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		});
		System.out.println("Finished Filling in tree");

		//nodes.addAll(set);*/
	}
}
