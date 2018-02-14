package etl.data.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.*;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.export.entities.Entity;

public class ETLFileWriter {
	public static String WRITE_PROCESSING_DIR = "";
	
	public static String WRITE_COMPLETED_DIR = ""; 
	
	private static Map<String,Set<Integer>> ENTITY_HASH_INDEX = new HashMap<String,Set<Integer>>();
	
	private static Set<Entity> WRITTEN_ENTITIES = new HashSet<Entity>();
	
	private Map<String,BufferedWriter> writerMap = new HashMap<String, BufferedWriter>();
	
	public Map<String, BufferedWriter> getWriterMap() {
		return writerMap;
	}

	public void setWriterMap(Map<String, BufferedWriter> writerMap) {
		this.writerMap = writerMap;
	}

	public void writeToFile(Map<String, Set<Entity>> entityMap) {	
		
		Map<String,BufferedWriter> writerMap = new HashMap<String, BufferedWriter>();
		// prep buffers.  Need to keep buffers open.  Placing them in a map and handling them as below keeps them open and will
		// close all buffers in the map in the finally clause. Probably a bit overkill as stream should close them.
		try{
			entityMap.keySet().stream().forEach(key -> {
				
				try{
				
					String destPath = WRITE_PROCESSING_DIR + key + ".csv";
					
					if(!Files.exists(Paths.get(destPath))){
						
						File file = new File(destPath);
						
						file.createNewFile();
					} else {
						//delete old file in processing
						Files.delete(Paths.get(destPath));
						
						File file = new File(destPath);
						
						file.createNewFile();
						
					}
					
					writerMap.put(key, Files.newBufferedWriter(Paths.get(destPath), StandardCharsets.UTF_8, StandardOpenOption.APPEND));
				
				} catch (Exception e) {
					
				}
			});
		
		
			
			entityMap.forEach((key,set) ->{
			
				String destPath = WRITE_PROCESSING_DIR + key + ".csv";
				
				set.stream().forEach(ent -> {
					try {
						BufferedWriter writer = writerMap.get(key);
						
						String text = ent.toCsv() + "\n";
	
						writer.write(text);
						
					} catch (Exception e) {
						
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				});
			
			});
		
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			
			writerMap.forEach((key,writer) -> {
				
				try {
					writer.close();
				} catch (Exception e) {
					System.err.println("writer failed to close");
					e.printStackTrace();
				}
			});
			
			
		}
		/* create a writer class to handle this
		for(Entity ent:entityMap){
			
			
			String text = ent.toCsv() + "\n";
			
			try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(destPath), StandardCharsets.UTF_8, StandardOpenOption.APPEND);){
			
				if(!Files.exists(Paths.get(destPath))){
										
					File file = new File(destPath);
					
					file.createNewFile();
				}
				
				writer.write(text);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
			
	}
	
	public void writeToFile(List<Entity> entities, boolean forceFlush) throws IOException {
	
		
		for(Entity entity: entities){

			/*if(isRecordWritten(entity)){
				continue;
			}*/
			
			String _className = entity.getClass().getSimpleName();
			
			
			String destPath = WRITE_PROCESSING_DIR + _className + ".txt";

			String text = entity.toCsv() + "\n";
						
			if(!Files.exists(Paths.get(destPath))){
				
				File file = new File(destPath);
				file.createNewFile();
				
			}
			
			if(writerMap.containsKey(_className)){
				
					
					BufferedWriter writer = writerMap.get(_className);
					writer.write(text);
					//addToHashIndex(entity);
					
			} else {
					
					BufferedWriter writer = Files.newBufferedWriter(Paths.get(destPath), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
					writer.write(text);
					writerMap.put(_className, writer);
					//addToHashIndex(entity);
					
			} 
			
		}
		
		if(forceFlush == true){
			
			for(BufferedWriter bw: writerMap.values()){
				bw.flush();
				//bw.close();
			}
			
		}
		
		for(BufferedWriter bw: writerMap.values()){
			bw.close();
			//bw.close();
		}
	}

	public void writeToFile(Set<Entity> entities, boolean forceFlush) throws IOException {
	
		
		for(Entity entity: entities){

			/*if(isRecordWritten(entity)){
				continue;
			}*/
			
			String _className = entity.getClass().getSimpleName();
			
			String destPath = WRITE_PROCESSING_DIR + _className + ".csv";

			String text = entity.toCsv() + "\n";
						
			if(!Files.exists(Paths.get(destPath))){
				
				File file = new File(destPath);
				file.createNewFile();
				
			}
			
			if(writerMap.containsKey(_className)){
				
					
					BufferedWriter writer = writerMap.get(_className);
					writer.write(text);
					//addToHashIndex(entity);
					
			} else {
					
					BufferedWriter writer = Files.newBufferedWriter(Paths.get(destPath), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
					writer.write(text);
					writerMap.put(_className, writer);
					//addToHashIndex(entity);
					
			} 
			
		}
		
		if(forceFlush == true){
			
			for(BufferedWriter bw: writerMap.values()){
				bw.flush();
				//bw.close();
			}
			
		}
		
	}
	
	private boolean isRecordWritten(Entity entity){
		/*0
		if(ENTITY_HASH_INDEX.containsKey(entity.getClass().getSimpleName())){
			if(ENTITY_HASH_INDEX.get(entity.getClass().getSimpleName()).contains(entity.hashCode())){
				// Object has already been written and is a duplicate - minus has collisions.  Which need to be recorded and handled separately
				return true;
			}
		}*/
		
		if(WRITTEN_ENTITIES.contains(entity)){
			return true;
		}
		
		return false;
	}
	
	private void addToHashIndex(Entity entity){
		/*
		if(ENTITY_HASH_INDEX.containsKey(entity.getClass().getSimpleName())){
			
			ENTITY_HASH_INDEX.get(entity.getClass().getSimpleName()).add(entity.hashCode());
			
		} else {
			
			ENTITY_HASH_INDEX.put(entity.getClass().getSimpleName(), new HashSet<Integer>(Arrays.asList(entity.hashCode())));
			
		}*/
		WRITTEN_ENTITIES.add(entity);
	}
	
	public void writeToFile(String str, String destination) throws IOException {
		
		if(!Files.exists(Paths.get(destination))){
			
			File file = new File(destination);
			
			file.createNewFile();
		}
			
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(destination), StandardCharsets.UTF_8, StandardOpenOption.APPEND);){
		
			
			writer.write(str);
			
		} 
		
	}
}
