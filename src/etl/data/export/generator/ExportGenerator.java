package etl.data.export.generator;

import java.util.Map;

public abstract class ExportGenerator{
	
	public abstract void doGenerate(Map<String,String> parameters);
	
}
