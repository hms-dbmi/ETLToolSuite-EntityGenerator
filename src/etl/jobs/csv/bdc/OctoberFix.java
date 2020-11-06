package etl.jobs.csv.bdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OctoberFix extends BDCJob {

	public static class Record {
		String patientNum = ""; //0
		String concept = ""; //1
		String empty = "";
		String accessionConsent = ""; 
		String accession = "";
	 
		public Record(String[] rec) {
			this.patientNum = rec[0];
			this.concept = rec[1];
			this.empty = rec[2];
			this.accessionConsent = rec[3];
			this.accession = rec[4];
					
		}

		@Override
		public String toString() {
			return "Record [patientNum=" + patientNum + ", concept=" + concept + ", empty=" + empty
					+ ", accessionConsent=" + accessionConsent + ", accession=" + accession + "]";
		}

		public String toStringArray(Record rec) {
			return new String(rec.patientNum + "," + rec.concept + "," + rec.empty + "," + rec.accessionConsent);
		}
	}
	
	
	
	
	public static void main(String[] args) throws IOException {
		
		List<String[]> accessionFilter = new ArrayList<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "mapping_list.txt"))) {
		
			String line;
			
			while((line = buffer.readLine()) != null) {
				accessionFilter.add(line.split(","));
			}
		}
		
		ArrayList<Record> globalConcepts = new ArrayList<>();
		
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_DIR + "tailglobal.csv"))) {
			
			String line;
			
			while((line = buffer.readLine()) != null) {
				String[] record = line.split(",");
				
				String patientNum = record[0]; //0
				String concept = record[1]; //1
				String empty = record[2]; //2
				String accessionConsent = record[3]; //3 
				String accession = record[3].split("\\.")[0]; //4
				
				String [] str = new String[] { patientNum, concept, empty, accessionConsent, accession };
				
				Record rec = new Record(str);
				Record rec2 = new Record(str);
				
				if(concept.startsWith("\\_consents")) {
					globalConcepts.add(rec);
						
					rec2.concept = "\\_Consents\\Short Study Accession with Consent Code\\";
					
					globalConcepts.add(rec2);
					
				} else if(concept.startsWith("\\_topmed_consents")) {
					rec.concept = "\\_consents\\";
					
					globalConcepts.add(rec);
					
					rec2.concept = "\\_Consents\\Short Study Accession with Consent Code\\";
					
					globalConcepts.add(rec2);
				}
				
			}
		}
		
		ArrayList<Record>  _consentsFiltered = new ArrayList<>();
		
		ArrayList<Record>  _unfiltered = new ArrayList<>();
		
		
		for(Record globalConcept: globalConcepts) {

			_unfiltered.add(globalConcept);
			if(inFilter(globalConcept.accession, accessionFilter)) {
				_consentsFiltered.add(globalConcept);
			}
			
		}
		
		System.out.println(_unfiltered.size());
		System.out.println(_consentsFiltered.size());
		System.out.println("here");
	
		Set<String> patientNums = new HashSet <String>();
		int x = 0;
		for(Record r: _consentsFiltered) {
				
			if(patientNums.contains(r.patientNum)) {
				System.err.println("duplicate id found " + r.patientNum );
				
			} else {
				if(r.concept.equals( "\\_Consents\\Short Study Accession with Consent Code\\")) {

					patientNums.add(r.patientNum);
				
				}
			}
		}
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "_octconsents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(Record out: _consentsFiltered ) {
				writer.write(out.toStringArray(out) + '\n');
			}
			
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(WRITE_DIR + "_octconsents.csv"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			for(Record out: _consentsFiltered ) {
				writer.write(out.toStringArray(out) + '\n');
			}
			
		}
	}

	private static boolean inFilter(String string, List<String[]> accessionFilter) {
		for(String[] filter: accessionFilter) {
			if(filter[1].equalsIgnoreCase(string)) return true;
		}
		return false;
	}

	
	
}
