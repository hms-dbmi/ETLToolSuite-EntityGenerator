package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.CsvToBean;

import etl.job.entity.i2b2tm.I2B2;
import etl.job.entity.i2b2tm.TableAccess;
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
			
			nodes = new HashSet<I2B2>(csvToBean.parse());
			
			fillTree(nodes, clevel + 1, clevel + 2);
			
			// build tableaccess entities
			tableAccess.addAll(TableAccess.createTableAccess(nodes));
		}
		try(BufferedWriter buffer = Files.newBufferedWriter(
				Paths.get(WRITE_DIR + File.separatorChar + "I2B2.csv"))){
			Utils.writeToCsv(buffer, nodes, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 		
		try(BufferedWriter buffer = Files.newBufferedWriter(
				Paths.get(WRITE_DIR + File.separatorChar + "TableAccess.csv"))){

			Utils.writeToCsv(buffer, tableAccess, DATA_QUOTED_STRING, DATA_SEPARATOR);
		} 		
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
	public static void fillTree(Set<I2B2> nodes, int clevelBegOccurance, int clevelEndOccurance) throws Exception{
		
		Set<I2B2> set = new HashSet<I2B2>();
		/*
		ConcurrentHashMap<CharSequence,Set<CharSequence>> trees = new ConcurrentHashMap<CharSequence, Set<CharSequence>>();
		
		nodes.forEach(node ->{
			
			CharSequence cfullname = node.getcFullName();
			if(StringUtils.countMatches(cfullname, "\\") <= 2) return; // is empty node or only base node.  No need to fill.

			int clevelBegIndex = StringUtils.ordinalIndexOf(cfullname, "\\", clevelBegOccurance);
			
			int clevelEndIndex = StringUtils.ordinalIndexOf(cfullname, "\\", clevelEndOccurance);
			
			CharSequence clevelName = cfullname.subSequence(clevelBegIndex, clevelEndIndex + 1);
			
			if(trees.containsKey(clevelName)) {
				trees.get(clevelName).add(cfullname);
			} else {
				trees.put(clevelName, new HashSet<CharSequence>(Arrays.asList(cfullname)));
			}
			
		});
		
		trees.entrySet().parallelStream()
		*/
		
		nodes.forEach(node ->{
			
			Integer x = StringUtils.countMatches(node.getcFullName(), PATH_SEPARATOR) - 1;
			
			while(x > 1){
				I2B2 i2b2 = null;
				try {
					i2b2 = (I2B2) node.clone();
				} catch (CloneNotSupportedException e) {
					System.err.println(e);
				}
				if(i2b2 == null) {
					break;
				}
				
				i2b2.setcFullName(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), PATH_SEPARATOR, x) + 1 ));
				
				i2b2.setcDimCode(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), PATH_SEPARATOR, x) + 1 ));
				
				i2b2.setcToolTip(node.getcFullName().substring(0, StringUtils.ordinalIndexOf(node.getcFullName(), PATH_SEPARATOR, x) + 1 ));
				
				i2b2.setcHlevel(new Integer(x - 2).toString());
				i2b2.setcBaseCode(null);
				i2b2.setcVisualAttributes("FA");
				
				i2b2.setcMetaDataXML("");
				
				String[] fullNodes = i2b2.getcFullName().split(PATH_SEPARATOR.toString());
				
				i2b2.setcName(fullNodes[fullNodes.length - 1]);
				
				if(node.getcHlevel() != null) {
					set.add(i2b2);
				}
				
				
				
				x--;
			}
		});

		nodes.addAll(set);
	}
}
