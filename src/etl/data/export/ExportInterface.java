package etl.data.export;

import java.util.List;
import java.util.Map;

import etl.data.datatype.DataType;
import etl.data.export.entities.Entity;
import etl.job.jsontoi2b2tm.entity.Mapping;

public interface ExportInterface {
	enum VALID_TABLES{};
	
	enum VALID_EXPORT_TYPE{};

	void generateExport();

	//<T> void generateTables(Map map, T mapping) throws InstantiationException, IllegalAccessException;
		
}
