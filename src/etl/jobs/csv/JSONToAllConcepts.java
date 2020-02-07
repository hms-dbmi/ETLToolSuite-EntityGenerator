package etl.jobs.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;

import etl.data.datasource.JSONDataSource2;
import etl.data.datatype.DataType;
import etl.job.entity.Mapping;
import etl.job.entity.hpds.AllConcepts;
import etl.jobs.jobproperties.JobProperties;

public class JSONToAllConcepts extends Job {
	
	private static final Class DATASOURCE_FORMAT = null;
	private static String RELATIONAL_KEY = null;
	private static Map<String,Integer> SEQUENCE_MAP = new HashMap<String,Integer>();
	private static boolean USE_PATIENT_MAPPING = false;

	private static Map<String,List<String>> OMISSIONS_MAP = new HashMap<String, List<String>>();
	{
	
		OMISSIONS_MAP.put("fields:activestatus", Arrays.asList("A"));
		OMISSIONS_MAP.put("fields:encoded_relation", Arrays.asList("null","",null));
	};
	
	//private static List<AllConcepts> ac = new ArrayList<AllConcepts>();
	private static Set<AllConcepts> ac = new TreeSet<AllConcepts>( new Comparator<AllConcepts>() {
		@Override
		public int compare(AllConcepts o1, AllConcepts o2) {
			if(o1 == null || o2 == null) return -1;
			
			int conceptpath = 0;
			if(o1.getTvalChar() == null) o1.setTvalChar("");
			if(!o1.getTvalChar().isEmpty()) {
				conceptpath = (o1.getConceptPath() + o1.getTvalChar()).compareTo((o2.getConceptPath() + o2.getTvalChar()));
			} else {
				conceptpath = (o1.getConceptPath() + o1.getNvalNum()).compareTo((o2.getConceptPath() + o2.getNvalNum()));

			}
			
			if(conceptpath != 0) {
				return conceptpath;
			}
			
			return o1.getPatientNum().compareTo(o2.getPatientNum());
									
		}
		
	} );
	 
	/**
	 * Standalone Main so this class can generate concepts alone.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			e.printStackTrace();
			System.err.println(e);
		}
		
		
		try {
			execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		
	}

	private static void execute() throws Exception {
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_allConcepts.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)){
		}
		List<Mapping> mappingFile = Mapping.class.newInstance().generateMappingList(MAPPING_FILE, MAPPING_SKIP_HEADER, MAPPING_DELIMITER, MAPPING_QUOTED_STRING);
		
		List records =  buildRecordList();
		
		if(USE_PATIENT_MAPPING) {
			buildSeqMap();
		}
		
		for(Object record: records) {
			
			if(record instanceof LinkedHashMap) {
				
				processEntities(mappingFile,(LinkedHashMap) record);
				
			}
			
		};

		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_allConcepts.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)){
			
			for(AllConcepts a: ac) {
				
				if(a.getNvalNum() == null) a.setNvalNum("");
				if(a.getTvalChar() == null) a.setTvalChar("");
				
				String val = a.getTvalChar();
				
				val = val.replaceAll("[^\\x00-\\x7F]", "");
				val = val.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
				val = val.replaceAll("\\p{C}", "");
				
				val.trim();
				a.setTvalChar(val);
				
				String cp = a.getConceptPath();
				cp = cp.replaceAll("[^\\x00-\\x7F]", "");
				cp = cp.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
				cp = cp.replaceAll("\\p{C}", "");
				
				a.setConceptPath(cp);
						
				writer.write(a.toCSV());
				writer.flush();
				
			}
			
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + TRIAL_ID + "_PatientMapping.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)){
			
			for(Entry<String,Integer> seqPat: SEQUENCE_MAP.entrySet()) {
				
				StringBuilder sb = new StringBuilder();
				
				sb.append('"');
				sb.append(seqPat.getKey());
				sb.append("\",\"");
				sb.append(TRIAL_ID);
				
				sb.append("\",\"");
				                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
				sb.append(seqPat.getValue());
				sb.append("\"\n");

				writer.write(sb.toString().toCharArray());
				writer.flush();
				
			}
			
		}
		
		System.out.println("Omitted " + xx );
		System.out.println("Not Omitted: " + y);
		
	}
	
	private static void buildSeqMap() throws IOException {
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(PATIENT_MAPPING_FILE))){
			
			CSVReader reader = new CSVReader(buffer);
			
			String[] line;
			
			while((line = reader.readNext()) != null) {
				if(line.length != 3)  continue;
				
				SEQUENCE_MAP.put(line[0], new Integer(line[2]));
				
			}
			
		}
		
	}

	private static List buildRecordList() throws Exception{
		
		if(!Files.isDirectory(Paths.get(DATA_DIR))) {
			return JSONDataSource2.buildObjectMap(new File(DATA_DIR), DATASOURCE_FORMAT);	
		}
				
		return null;
	}
	
	static int xx = 0;
	static int y = 0;
	
	private static void processEntities(List<Mapping> mappings, Map record) throws Exception{
		
		boolean isOmitted = isOmitted(record);

		if(isOmitted) {
			xx++;
		} else {
			y++;
		}

		@SuppressWarnings("unused")
		String relationalValue = findValueByKey2(record,new ArrayList(Arrays.asList(RELATIONAL_KEY.split(":")))).get(0).toString();
	
		if(!isOmitted){
			
			Integer patNum = sequencePatientNum(relationalValue.toString());

			for(Mapping mapping: mappings){
				
				DataType dt = DataType.initDataType(StringUtils.capitalize(mapping.getDataType()));
				
				if(!mapping.getDataType().equalsIgnoreCase("OMIT")){
					
					ArrayList<String> key = new ArrayList(Arrays.asList(mapping.getKey().split(":")));
					// remove file name
					key.remove(0);
					
					List<Object> values = findValueByKey2(record, key);
					
					Map<String,String> options = mapping.buildOptions(mapping);
					
					if(!options.isEmpty()) {
						
						if(options.containsKey("TYPE")) {
							
							String type = options.get("TYPE");
							
							if(type.equalsIgnoreCase("datediffin")) {

								String dateDiffFrom = options.containsKey("DIFFFROM") ? options.get("DIFFFROM").replaceAll("-",":"): null;
								
								String mask = options.containsKey("MASK") ? options.get("MASK"): null;

								String diffIn = options.containsKey("DIFFIN") ? options.get("DIFFIN"): null;
								
								LocalDate startDateInclusive = null;
								
								LocalDate endDateExclusive = null;
								
								if(dateDiffFrom.equalsIgnoreCase("sysdate")) {
									
									startDateInclusive = LocalDate.now();
									
								} else {
									
									Object fromO = 
											findValueByKey(new LinkedHashMap(record), dateDiffFrom.split(":")).get(0);

									if(mask != null) {
										if(fromO != null) {
											
											String from = fromO.toString();
											
											startDateInclusive = LocalDate.parse(from, DateTimeFormatter.ofPattern(mask) );
											
										}
										
									} else throw new Exception("No mask given");	
									
								}
								for(int x = 0; x < values.size(); x++) {
									
									String value = values.get(0).toString();
									
									endDateExclusive = value.isEmpty() ? null: LocalDate.parse(value, DateTimeFormatter.ofPattern(mask) );
									
									if(endDateExclusive != null) {
										
										Period period = Period.between(endDateExclusive, startDateInclusive );

										values.remove(x);
										
										if(diffIn.equalsIgnoreCase("years")) {
											values.add(x, period.getYears());
										}
									}								
								}
							} else if(type.equalsIgnoreCase("datedifffrom")) {
								
								String dateDiffFrom = options.containsKey("DIFFFROM") ? options.get("DIFFFROM").replaceAll("-",":"): null;
								
								String mask = options.containsKey("MASK") ? options.get("MASK"): null;

								String diffIn = options.containsKey("DIFFIN") ? options.get("DIFFIN"): null;
								
								LocalDate startDateInclusive = null;
								
								LocalDate endDateExclusive = null;
								
								if(dateDiffFrom.equalsIgnoreCase("sysdate")) {
									startDateInclusive = LocalDate.now();
								} else {
									
									Object fromO = 
											findValueByKey(new LinkedHashMap(record), dateDiffFrom.split(":")).get(0);

									if(mask != null) {
										if(fromO != null) {
											
											String from = fromO.toString();
											
											startDateInclusive = LocalDate.parse(from, DateTimeFormatter.ofPattern(mask) );
											
										}
										
									} else throw new Exception("No mask given");							
								}
								for(int x = 0; x < values.size(); x++) {
									if(values.get(0) != null) {
										String value = values.get(0).toString();
										
										endDateExclusive = value.isEmpty() ? null: LocalDate.parse(value, DateTimeFormatter.ofPattern(mask) );
										
										if(endDateExclusive != null) {
											
											Period period = Period.between(startDateInclusive, endDateExclusive);
											
											values.remove(x);
											if(diffIn.equalsIgnoreCase("years")) {
												values.add(x, period.getYears());
											}
										}					
									}
								}							
							}
						}
					}
					Mapping m2 = mapping;
					
					if(m2.getKey().toCharArray()[0] == ':') m2.setKey(m2.getKey().replaceFirst(":", ""));

					Set<AllConcepts> newEnts = dt.generateTables(m2, null, values, Arrays.asList(patNum));
					
					if(newEnts != null && newEnts.size() > 0){
					
						ac.addAll(newEnts);
					
					}
					
				}
						
			}

		}
	}
	
	private static Integer sequencePatientNum(String preSeqPatientNum) {
		
		if(SEQUENCE_MAP.containsKey(preSeqPatientNum)) {
			
			return SEQUENCE_MAP.get(preSeqPatientNum);
			
		} else {
			Integer postSeqPatientNum = PATIENT_NUM_STARTING_SEQ;

			SEQUENCE_MAP.put(preSeqPatientNum, PATIENT_NUM_STARTING_SEQ);
			
			PATIENT_NUM_STARTING_SEQ++;
			
			return postSeqPatientNum;
		}
		
	}

	private static List<Object> findValueByKey2(Object record, ArrayList<String> keys) throws Exception {
		
		List<Object> list = new ArrayList<Object>();
		
		if( !keys.isEmpty() && keys.size() > 0 ) {
			
			String k = keys.get(0);
			// presume it must be a map
			if(record instanceof Map) {
				keys.remove(0);
				
				Map<String,Object> record2 = (LinkedHashMap) record;
				
				if(record2.containsKey(k)) {
					
					Object o = record2.get(k);

					list = findValueByKey2(o, keys);
				}
			} else if ( record instanceof List) {
				
				List recs = (ArrayList<Object>) record;

				for(Object o: recs) {
					
					list = findValueByKey2(o, keys);
					
				}
				
				
			} 
		} else if( record instanceof Collection){
			list.addAll((Collection<? extends Object>) record);
		} else {
			list.add(record);
		}
		return list;
		
	}
	@SuppressWarnings("unchecked")
	private static List<Object> findValueByKey(Map record, String[] key) throws Exception {
		
		Map record2 = new LinkedHashMap(record);
		
		Iterator<String> iter = new ArrayList(Arrays.asList(key)).iterator();
		while(iter.hasNext()) {
			
			String currKey = iter.next();
			
			if(record2.containsKey(currKey)){
				
				Object obj = record2.get(currKey);
				
				if(obj == null) {

					return new ArrayList<Object>();
					
				}
				if(obj instanceof Map) {
					
					record2 = new LinkedHashMap((LinkedHashMap)obj);
					
					// if last key return a the hashmap of values to be processed by a datatype
					if(!iter.hasNext()) {
						
						List<Object> l = new ArrayList<Object>();
						
						l.add(record2);
						
						return l;
					
					}
					
				} else if ( obj instanceof String || obj instanceof Boolean ) {

					return new ArrayList(Arrays.asList(obj));
					
				} else if ( obj instanceof List ) {
					
					List<Object> l = new ArrayList<Object>();
					
					for(Object o: (List) obj) {
						if(!iter.hasNext()) {

							if(o instanceof Map) {
								
								record2 = new LinkedHashMap((LinkedHashMap) o);
								
								l.add(record2);
							
							}
						} else {
							return (ArrayList) obj;
						}
					}
					return l;
				} 
			} 
		}
		return null;
	}
	
	private static boolean isOmitted(Map record) throws Exception {
		boolean isOmitted = false;
		if(!OMISSIONS_MAP.isEmpty()) {
			if(record != null || !record.isEmpty()) {
				for(String omkeyfull: OMISSIONS_MAP.keySet()) {
					
					String[] keyArr = omkeyfull.split(":");
					String key = keyArr[keyArr.length - 1];
					List<Object> recordVals = findValueByKey2(record, new ArrayList<String>(Arrays.asList(keyArr)));
					
					String omissionVal = OMISSIONS_MAP.get(omkeyfull).get(0).toString();
					
					if(recordVals == null || recordVals.isEmpty()){

						if(omissionVal == null) {
							isOmitted = false;
							
						}
						else if(omissionVal.equalsIgnoreCase("null")) {
							isOmitted = false;
							
						}
						else if(omissionVal.isEmpty()) {
							isOmitted = false;
							
						}
						else {
							isOmitted = true;
							
						}
					} else {
						for(Object recVal: recordVals) {
							if(recVal == null) {
								if(omissionVal == null) {
									isOmitted = false;
									
								}
								else if(omissionVal.equalsIgnoreCase("null")) {
									isOmitted = false;
									
								}
								else if(omissionVal.isEmpty()) {
									isOmitted = false;
									
								}
								else {
									isOmitted = true;
								}
							} else if(recVal.toString().equalsIgnoreCase(omissionVal)) {
								isOmitted = false;
							} else {
								isOmitted = true;
							}
						}
					}
					if(isOmitted == true) break;
				}		
			
			}
		} 
		return isOmitted;
	}
	private static boolean isOmitted(ArrayList<String> omkeys, Object record, String currFullKey) {
		
		if(record instanceof Map) {
			boolean isOmitted = false;
			Map<String, Object> rec = (LinkedHashMap<String,Object>) record;
			
			if(rec.keySet().contains(omkeys.get(0))) {
				Object r = rec.get(omkeys.get(0));
				
				if(r instanceof Map) {
					omkeys.remove(0);
					isOmitted = isOmitted(omkeys, r, currFullKey);
				} else {
					isOmitted = isOmitted(omkeys, r, currFullKey);
				}
			} else {
				isOmitted = true;
			}
			return isOmitted;
			
		} else if(record instanceof List) {
			List recs = (List) record;
			boolean isOmitted = false;

			for(Object r: recs) {
				isOmitted = isOmitted(omkeys, r, currFullKey);
				if(isOmitted == true) break;
			}
			return isOmitted;

		} else if(record instanceof String) {
			boolean isOmitted = false;

			List<String> l = OMISSIONS_MAP.get(currFullKey);
			if(l.contains(record.toString())) {
				isOmitted = false;
			} else {
				isOmitted = true;
			}
			
			return isOmitted;

		} else if(record == null) {
			boolean isOmitted = false;

			List<String> l = OMISSIONS_MAP.get(currFullKey);
			
			if(l.contains(null)) {
				isOmitted = false;
			} else {
				isOmitted = true;
			}
			return isOmitted;

		} else {
			return true;
		}
	}

	protected static void setVariables(String[] args, JobProperties properties) throws Exception {
		
		Job.setVariables(args, properties);
		
		if(properties != null) {
			
			RELATIONAL_KEY = properties.contains("relationalkey") ? properties.getProperty("relationalkey").toString() : RELATIONAL_KEY;
			
			if(properties.contains("omissions")) setOmissionsMap(properties);
			
			if(properties.contains("usepatientmapping")) {
				
				if(properties.get("usepatientmapping").toString().equalsIgnoreCase("Y") || properties.get("usepatientmapping").toString().equalsIgnoreCase("YES")|| properties.get("usepatientmapping").toString().equalsIgnoreCase("TRUE")) {
					
					PATIENT_MAPPING_FILE = properties.contains("patientmappingfile") ? properties.getProperty("patientmappingfile"): "";
					
					if(!Files.exists(Paths.get(PATIENT_MAPPING_FILE))) {
						
						USE_PATIENT_MAPPING = false;
						
						System.err.println(PATIENT_MAPPING_FILE + " does not exist!");
						
					} else {
						
						USE_PATIENT_MAPPING = true;
						
					}
				}
			} else {
				USE_PATIENT_MAPPING = false;
			}
		}
	}

	private static void setOmissionsMap(JobProperties properties) {
		
		String omissionsDenormalized = properties.get("omissions").toString();
		
		String[] omissionsArray = omissionsDenormalized.split("\\|");
		
		for(String omission: omissionsArray) {
			String[] kvpair = omission.split(";");
			if(kvpair.length == 2) {
				OMISSIONS_MAP.put(kvpair[0], Arrays.asList(kvpair[1]));
			}
		}	
	}
	
}
