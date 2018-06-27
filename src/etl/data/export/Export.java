package etl.data.export;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

	public static Map<Path, List<String>> buildFilestoWrite(Set<Entity> entities, String writeDir, String outputExt) throws IOException{
		Map<Path, List<String>> map = new HashMap<Path,List<String>>();
		
		if(entities != null) {
			for(Entity entity: entities) {
				Path destPath = Paths.get(writeDir + entity.getClass().getSimpleName() + outputExt);
				
				if(!map.containsKey(destPath)) {
					
					List<String> list = new ArrayList<String>();
					list.add(entity.toCsv());
					map.put(destPath, list);
					
				} else {
					List<String> list = map.get(destPath);
					list.add(entity.toCsv());
					map.put(destPath, list);
					
				}
			}
		}
		
		return map;
	}
	
	public static void writeToFile(Map<Path, List<String>> filesToWrite, OpenOption[] options) throws IOException{
		Map<Path,BufferedOutputStream> outs = new HashMap<Path,BufferedOutputStream>();
		
		if(filesToWrite != null ) {
			for(Path path:filesToWrite.keySet()){
				if(outs.containsKey(path)) {
					BufferedOutputStream out = outs.get(path);
					List<String> data = filesToWrite.get(path);
					for(String line: data) {
						line = line + '\n';
						out.write(line.getBytes());		
						out.flush();
					}
				} else {
					BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(path, options));
					
					outs.put(path, out);
					
					List<String> data = filesToWrite.get(path);
					for(String line: data) {
						line = line + '\n';
						out.write(line.getBytes());
						out.flush();

					}
					
				}
				//Files.write(path, filesToWrite.get(path), encoding, options );
			}
			/*
			filesToWrite.entrySet().parallelStream().forEach(entry -> {
				try {
					Files.write(entry.getKey(), entry.getValue(), encoding, options );
				} catch (IOException e) {
					
				}
			});
			*/
	//		Path destPath = Paths.get(destination);
			
	//		Files.write(destPath, Arrays.asList(str), encoding, options );
		
		}	
	}
	
	public static void writeToFile(String destination, String str, OpenOption[] options) throws IOException{
		if(str != null) {
		Path destPath = Paths.get(destination);
		
		Files.write(destPath, Arrays.asList(str), encoding, options );
		}
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
