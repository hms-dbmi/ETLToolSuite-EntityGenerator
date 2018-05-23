package etl.data.datatype.i2b2.objectarray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sound.midi.SysexMessage;

import etl.data.datatype.i2b2.Objectarray;
import etl.data.export.entities.Entity;
import etl.data.export.entities.i2b2.ConceptDimension;
import etl.data.export.entities.i2b2.I2B2;
import etl.data.export.entities.i2b2.ModifierDimension;
import etl.data.export.entities.i2b2.ObservationFact;
import etl.job.jsontoi2b2tm.entity.Mapping;

public class Modifier extends Objectarray {

	public Modifier(String dataType) {
		super(dataType);
		// TODO Auto-generated constructor stub
	}
	@Override
	public  Set<Entity> generateTables(Mapping mapping, List<Entity> entities, List<Object> values,
			List<Object> relationalValues) throws Exception{
		
		Set<Entity> ents = new HashSet<Entity>();
		
		Map<String, String> options = mapping.buildOptions(mapping);
		
		Map<String, String> labels = makeKVPair(options.get("MODLABEL"),"\\|","=");

		List<String> modRequiredNode = Arrays.asList(options.get("MODIFIERVALUE").split("\\|"));
		
		Set<String> textKeys = options.containsKey("MODIFIERS") ? generateKeys(options.get("MODIFIERS"), mapping.getKey()): new HashSet<String>();
		
		if(values == null) return new HashSet<Entity>();
		
		for(Object v: values) {
			
			if(!(v instanceof Map)) {
				
				continue;
				
			}
			
			Map<String,Object> vmap = (HashMap<String,Object>) v;
			
			Boolean hasValueField = false;
			String reqNode = "";
			// make sure record has valuefield needed to display
			String conceptNode = new String();
			String currReqVal = new String();
			for(String reqNode2:modRequiredNode) {
				
				hasValueField = vmap.containsKey(reqNode2) ? true: false;
				
				if(hasValueField) {
					
					reqNode = vmap.get(reqNode2).toString();
					
					conceptNode = reqNode2;
					
					currReqVal = vmap.get(reqNode2).toString();
					
					break;	
				}
			
			}
			if(labels.containsKey(conceptNode)) conceptNode = labels.get(conceptNode);
			// skip record if missing required field
			if(!hasValueField) continue;
			
			for(Object relationalvalue: relationalValues) {
				
				for(String label: labels.keySet()) {
				
					if(vmap.containsKey(label)) {
						
						List<Object> vals = new ArrayList(Arrays.asList(vmap.get(label)));
						
						for(Object val:vals) {
							
							if(val == null) continue; //System.out.println(label + ':' + val);
							String value = val.toString();
							
							boolean isNumeric = value.matches("[-+]?\\d*\\.?\\d+") ? true : false; 
							
							label = labels.containsKey(label) ? labels.get(label): label;
							
							for(Entity entity: entities){
								
								if(entity instanceof I2B2){ 
									
									// OAE IF HAS A CONTAINER (ROOT)
									if(options.get("ROOTNODE") != null && !options.get("ROOTNODE").isEmpty()){
										
										I2B2 i2b2 = new I2B2("I2B2");
	
										List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE")));
										
										i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList) + 1).toString());
										
										i2b2.setcFullName(Entity.buildConceptPath(pathList));
										
										i2b2.setcName(options.get("ROOTNODE"));
										
										i2b2.setcVisualAttributes("OAE");
										
										i2b2.setcFactTableColumn("MODIFIER_CD");
	
										i2b2.setcTableName("MODIFIER_DIMENSION");
										
										i2b2.setcColumnName("MODIFIER_PATH");
	
										i2b2.setcColumnDataType("T");
										
										i2b2.setcOperator("LIKE");
										
										i2b2.setcDimCode(i2b2.getcFullName());
										
										i2b2.setcToolTip(i2b2.getcFullName());
										
										i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
										
										i2b2.setcMetaDataXML(C_METADATAXML);
																				
										i2b2.setmAppliedPath( Entity.buildConceptPath( new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label))) + "%" );
										
										ents.add(i2b2);

									}

								} 
													
								if(entity instanceof ObservationFact) {
									
									ObservationFact of = new ObservationFact("ObservationFact");

									of.setPatientNum(relationalvalue.toString());
									
									of.setEncounterNum(relationalvalue.toString() + value);
									
									of.setConceptCd(mapping.getRootNode() + ":" + "MODIFIER" + ":" + conceptNode + currReqVal);
									
									List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE"), label));
									
									of.setModifierCd(Entity.buildConceptPath(pathList).replace('\\', ':'));
									
									of.setValtypeCd("T");
															
									of.setTvalChar(value);
															
									of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

									ents.add(of);
															
								} 
								if(entity instanceof ModifierDimension){
	
									ModifierDimension md = new ModifierDimension("ModifierDimension");
														
									List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE"), label));
									
									md.setModifierCd(Entity.buildConceptPath(pathList).replace('\\', ':'));
									
									md.setModifierPath(Entity.buildConceptPath(pathList));
									
									md.setNameChar(label);
									
									md.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

									ents.add(md);
									
								} if(entity instanceof I2B2){ 
									
									I2B2 i2b2 = new I2B2("I2B2");
	
									List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE"), label));

									i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList) + 2).toString());
									
									i2b2.setcFullName(Entity.buildConceptPath(pathList));
									
									i2b2.setcDimCode(i2b2.getcFullName());
									
									i2b2.setcToolTip(i2b2.getcFullName());
									
									i2b2.setcName(label);
									
									i2b2.setcVisualAttributes("RA");
									
									i2b2.setcFactTableColumn("MODIFIER_CD");

									i2b2.setcTableName("MODIFIER_DIMENSION");
									
									i2b2.setcColumnName("MODIFIER_PATH");
	
									i2b2.setcColumnDataType("T");
									
									i2b2.setcOperator("LIKE");
										
									i2b2.setcMetaDataXML(C_METADATAXML);
									
									i2b2.setmAppliedPath("\\%");
									
									i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

									ents.add(i2b2);
									
								} 
								
								List<String> pathList3 = new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), conceptNode, currReqVal));

								if(entity instanceof I2B2) {
								
									I2B2 i2b2 = new I2B2("I2B2");

									i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList3)).toString());
									
									i2b2.setcFullName(Entity.buildConceptPath(pathList3));
									
									i2b2.setcDimCode(i2b2.getcFullName());
									
									i2b2.setcToolTip(i2b2.getcFullName());
									
									i2b2.setcName(currReqVal);
									
									i2b2.setcVisualAttributes("FA");
									
									i2b2.setcFactTableColumn("CONCEPT_CD");
									
									i2b2.setcTableName("CONCEPT_DIMENSION");
									
									i2b2.setcColumnName("CONCEPT_PATH");
	
									i2b2.setcColumnDataType("T");
									
									i2b2.setcOperator("LIKE");
									
									
									i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);									
						
									ents.add(i2b2);
									
								} if(entity instanceof ConceptDimension){
									
									ConceptDimension cd = new ConceptDimension("ConceptDimension");
									
									cd.setConceptCd(mapping.getRootNode() + ":" + "MODIFIER" + ":" + conceptNode + currReqVal);
									
									cd.setConceptPath(Entity.buildConceptPath(pathList3));
									
									cd.setNameChar(currReqVal);
									
									cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
									
									ents.add(cd);
									
								} if(entity instanceof ObservationFact) {
									
									ObservationFact of = new ObservationFact("ObservationFact");

									of.setPatientNum(relationalvalue.toString());
									
									of.setEncounterNum("MODIFIER");
									
									of.setConceptCd(mapping.getRootNode() + ":" + "MODIFIER" + ":" + conceptNode + currReqVal);
									
									of.setModifierCd("@");
									
									of.setValtypeCd("T");
															
									of.setTvalChar("");
															
									of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);

									ents.add(of);
								}
							}
						}
					}
				}
			}
		}
		return ents;
	}
	
	@Override
	public Set<Entity> generateTables(Map map, Mapping mapping,
			List<Entity> entities, String relationalKey, String omissionKey) throws InstantiationException, IllegalAccessException, Exception {
		// options
		Map<String, String> options = mapping.buildOptions(mapping);
		
		Map<String, String> labels = makeKVPair(options.get("MODLABEL"),"\\|","=");

		List<String> modRequiredNode = Arrays.asList(options.get("MODIFIERVALUE").split("\\|"));
		
		Set<Entity> ents = new HashSet<Entity>();
		
		for(Entity entity: entities){			
			
			if(map != null && map.containsKey(mapping.getKey()) && map.containsKey(relationalKey)){
				
				List<Map<String, List<String>>> mods = generateArray(map.get(mapping.getKey()));
				
				Set<String> textKeys = new HashSet<String>();
				
				if(options.containsKey("MODIFIERS")){

					textKeys = generateKeys(options.get("MODIFIERS"), mapping.getKey());

				} else { 
					
					textKeys = generateKeys(map, mapping.getKey());
				
				}
				
				for(Map<String, List<String>> mod: mods){
					
					String conceptNode = "Name";
					String currDrug = "";

					for(String reqKey: modRequiredNode){
						if(mod.containsKey(reqKey)){
							
							conceptNode = "Name";
							currDrug = mod.get(reqKey).get(0);
						}
												
					}
					for(String str: textKeys){
						
						if(map.get(mapping.getKey()) != null && !map.get(mapping.getKey()).toString().isEmpty() && mod.containsKey(str)){

							try {
																		
									for(String s: mod.get(str)){

										String label = str;
										
										if(labels.containsKey(str)){
											
											label = labels.get(str);
											
										}
										boolean isRootNode = false;
										String keyRequired = "";
										
										
										for(String reqKey: modRequiredNode){
											
											if(str.equals(reqKey)){
												
												isRootNode = true;
												keyRequired = "CONCEPTNODE";
												
												
											} else {
												
												isRootNode = false;
												
											}
											
										}
										List<String> pathList2 = new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label, s));
										
										if(entity instanceof I2B2){ 
	
											// OAE IF HAS A CONTAINER (ROOT)
											if(options.get("ROOTNODE") != null && !options.get("ROOTNODE").isEmpty()){
												
												I2B2 i2b2 = new I2B2("I2B2");
			
												List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE")));
												
												i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList) + 1).toString());
												
												i2b2.setcFullName(Entity.buildConceptPath(pathList));
												
												i2b2.setcName(options.get("ROOTNODE"));
												
												i2b2.setcVisualAttributes("OAE");
												
												i2b2.setcFactTableColumn("MODIFIER_CD");
			
												i2b2.setcTableName("MODIFIER_DIMENSION");
												
												i2b2.setcColumnName("MODIFIER_PATH");
			
												i2b2.setcColumnDataType("T");
												
												i2b2.setcOperator("LIKE");
												
												i2b2.setcDimCode(i2b2.getcFullName());
												
												i2b2.setcToolTip(i2b2.getcFullName());
												
												i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
												
												i2b2.setcMetaDataXML(C_METADATAXML);
																						
												i2b2.setmAppliedPath( Entity.buildConceptPath( new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), label))) + "%" );
												
												ents.add(i2b2);
		
											}
		
										} 
															
										if(entity instanceof ObservationFact) {
											
											ObservationFact of = new ObservationFact("ObservationFact");
		
											of.setPatientNum(map.get(relationalKey).toString());
											
											of.setEncounterNum(map.get(relationalKey) + s);
											
											of.setConceptCd(mapping.getRootNode() + ":" + "MODIFIER" + ":" + conceptNode + currDrug);
											
											List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE"), label));
											
											of.setModifierCd(Entity.buildConceptPath(pathList).replace('\\', ':'));
											
											of.setValtypeCd("T");
																	
											of.setTvalChar(s);
																	
											of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
		
											ents.add(of);
																	
										} 
										if(entity instanceof ModifierDimension){
			
											ModifierDimension md = new ModifierDimension("ModifierDimension");
																
											List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE"), label));
											
											md.setModifierCd(Entity.buildConceptPath(pathList).replace('\\', ':'));
											
											md.setModifierPath(Entity.buildConceptPath(pathList));
											
											md.setNameChar(label);
											
											md.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
		
											ents.add(md);
											
										} if(entity instanceof I2B2){ 
											
											I2B2 i2b2 = new I2B2("I2B2");
			
											List<String> pathList = new ArrayList<>(Arrays.asList(options.get("ROOTNODE"), label));
		
											i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList) + 1).toString());
											
											i2b2.setcFullName(Entity.buildConceptPath(pathList));
											
											i2b2.setcName(label);
											
											i2b2.setcVisualAttributes("RA");
											
											i2b2.setcTableName("MODIFIER_DIMENSION");
											
											i2b2.setcColumnName("MODIFIER_PATH");
			
											i2b2.setcColumnDataType("T");
											
											i2b2.setcOperator("LIKE");
												
											i2b2.setcMetaDataXML(C_METADATAXML);
											
											i2b2.setmAppliedPath("\\%");
											
											i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
		
											ents.add(i2b2);
											
										} 
										
										List<String> pathList3 = new ArrayList<>(Arrays.asList(mapping.getRootNode(), mapping.getSupPath(), conceptNode, currDrug));

										if(entity instanceof I2B2) {
										
											I2B2 i2b2 = new I2B2("I2B2");

											i2b2.setcHlevel(Entity.calculateHlevel(Entity.buildConceptPath(pathList3)).toString());
											
											i2b2.setcFullName(Entity.buildConceptPath(pathList3));
											
											i2b2.setcName(currDrug);
											
											i2b2.setcVisualAttributes("FA");
											
											i2b2.setcFactTableColumn("CONCEPT_CD");
											
											i2b2.setcTableName("CONCEPT_DIMENSION");
											
											i2b2.setcColumnName("CONCEPT_PATH");
			
											i2b2.setcColumnDataType("T");
											
											i2b2.setcOperator("LIKE");
												
											i2b2.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);									
								
											ents.add(i2b2);
											
										} if(entity instanceof ConceptDimension){
											
											ConceptDimension cd = new ConceptDimension("ConceptDimension");
											
											cd.setConceptCd(mapping.getRootNode() + ":" + "MODIFIER" + ":" + conceptNode + currDrug);
											
											cd.setConceptPath(Entity.buildConceptPath(pathList3));
											
											cd.setNameChar(currDrug);
											
											cd.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
											
											ents.add(cd);
											
										} if(entity instanceof ObservationFact) {
											
											ObservationFact of = new ObservationFact("ObservationFact");
		
											of.setPatientNum(map.get(relationalKey).toString());
											
											of.setEncounterNum("MODIFIER");
											
											of.setConceptCd(mapping.getRootNode() + ":" + "MODIFIER" + ":" + conceptNode + currDrug);
											
											of.setModifierCd("@");
											
											of.setValtypeCd("T");
																	
											of.setTvalChar("");
																	
											of.setSourceSystemCd(DEFAULT_SOURCESYSTEM_CD);
		
											ents.add(of);
									}
								}
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						}
						
					}
				}
			}
	
		}
		return ents;
	
	}
	
	private String getModValueNode(String string,Map map) {
		String[] strarr = string.split("\\|");
		
		for (String string2 : strarr) {
			if(map.get(string2) != null && !map.get(string2).toString().isEmpty()){
				return string2;
			}
		}
		
		return null;
	}


	private int findIndexOf(char[] carr, char ch, int indexof){
		int index = 0;
		int y = 1;
		
		if(indexof >= y){
			
			for(char c: carr){
				
				if(c == ch){
					
					if(indexof == y){
						
						return index;
						
					} else {
						
						y++;
					}
				}
				
				index++;
			}
		}
		
		return -1;
	}
}
