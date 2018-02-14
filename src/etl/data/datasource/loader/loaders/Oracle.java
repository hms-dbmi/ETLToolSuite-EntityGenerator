package etl.data.datasource.loader.loaders;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import etl.data.datasource.loader.Loader;import etl.data.export.entities.Entity;


public class Oracle extends Loader {

	public Oracle(String str) throws Exception {
		super(str);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Map<String, OracleControlFile> generateLoadTest(List<String> inFileNames) {

		Map<String, OracleControlFile> ctlFiles = new HashMap<String,OracleControlFile>();
		
		inFileNames.stream().forEach(fileName ->{
			OracleControlFile ocf = new OracleControlFile();
			
			ocf.setInFile(fileName + ".txt");
			ocf.setBadFile(fileName + ".bad");
			ocf.setDiscardFile(fileName + ".dsc");
			ocf.setAppend(true);
			Entity entity = Entity.initEntityType("etl.data.export.entities.i2b2.", fileName);
			ocf.setTable(entity.schema + "." + fileName);
			
			List<String> lines = new ArrayList<String>();
			Field[] fields = entity.getClass().getDeclaredFields();
			for(Field field: fields){
				lines.add(field.getName());
			}
			
			ocf.setFieldList(lines);
			
			ctlFiles.put(fileName, ocf);
		});
		
		
		return ctlFiles;
	}

	
}
