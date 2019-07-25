package etl.jobs.jobproperties;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JobProperties extends Properties{
	
	public List<String> REQUIRED_PROPERTIES = new ArrayList<String>();
	/**
	 * 
	 */
	private static final long serialVersionUID = 5987407489240934574L;
		
	
	
	public JobProperties() {
		super();
	}

	public JobProperties buildProperties(String propertiesFile) throws Exception {
			
		
		List<String> requiredfields = new ArrayList<String>();
		
		List<String> availableKeys = new ArrayList<String>();
		
		if(Files.exists(Paths.get(propertiesFile))) {

			this.load(new FileInputStream(propertiesFile));
			requiredfields.addAll(REQUIRED_PROPERTIES);
			
			for(Object key: this.keySet()) {
				
				availableKeys.add(key.toString());
				
			}
			
		}
		
		if(availableKeys.containsAll(requiredfields)) {
						
			return this;
			
		} else {
			
			StringBuilder missingFields = new StringBuilder(); 
			
			for(String field: requiredfields) {
				
				if(!availableKeys.contains(field)) {
					
					missingFields.append(field + ' ');
					
				}
				
			}
			
			throw new Exception("Missing Required Field in properties file " + missingFields.toString());
	
		}
	}
	
	/**
	 * Overriding superclass Properties methods that
	 *  put and gets properties to always use lowercase for the keys.
	 *  Users and developers do not have to worry about casing in properties.
	 *  Always use JobProperties as your generic class not Properties.
	 */
	@Override
	public boolean contains(Object value) {
		if(value == null) return false;
		if(this.stringPropertyNames().contains(value.toString())) return true;
		else return false;
		
	}
	@Override
    public Object put(Object key, Object value) {
        String lowercase = ((String) key).toLowerCase();
        return super.put(lowercase, value);
    }
    
    @Override
    public String getProperty(String key) {
        String lowercase = key.toLowerCase();
        return super.getProperty(lowercase);
    }
    @Override
    public String getProperty(String key, String defaultValue) {                              
        String lowercase = key.toLowerCase();
        return super.getProperty(lowercase, defaultValue);
    }
}
