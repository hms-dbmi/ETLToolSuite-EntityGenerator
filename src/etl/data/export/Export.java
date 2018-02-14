package etl.data.export;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datatype.DataType;
import etl.data.export.entities.Entity;
import etl.data.export.generator.ExportGenerator;
import etl.job.jsontoi2b2tm.entity.Mapping;


public abstract class Export implements ExportInterface {

	public static Charset encoding = StandardCharsets.UTF_8;
	
	private enum VALID_TYPES{ ExportI2B2 }
	
	public Export(String exportType){};
	

	@Override
	public void generateExport() {
		// TODO Auto-generated method stub

	}

	public abstract void generateTables(Map map, Mapping mapping, List<Entity> entities, String relationalKey, String omissionKey) throws InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException;
	
	public static boolean isValidExportType(String exportType) {
		
		for(VALID_TYPES v: VALID_TYPES.values()){
		
			if(v.toString().equals(exportType)) return true;
		
		}
		
		return false;
		
	}
	
	public static void writeToFile(String destination, String str, OpenOption[] options) throws IOException{
		Path destPath = Paths.get(destination);

		Files.write(destPath, Arrays.asList(str), encoding, options );
	}
	
	@SuppressWarnings("finally")
	public static Export initExportType(String exportPackage, String exportType){
		
		Export newInstance = null;

		try {		
			
			if(isValidExportType(exportType)){
				Class<?> resourceInterfaceClass = Class.forName(exportPackage + exportType);
	
				newInstance =  (Export) resourceInterfaceClass.getDeclaredConstructor(String.class).newInstance(exportType);
				
			} else {
				
				throw new Exception();
				
			}
			
		} catch (SecurityException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (InstantiationException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (IllegalAccessException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (ClassNotFoundException e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} catch (Exception e) {
			
			System.out.println(e);
			
			e.printStackTrace();
			
			return null;
			
		} finally {
			
			return newInstance;
		}
		
	}
	
}
