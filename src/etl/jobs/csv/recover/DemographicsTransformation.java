package etl.jobs.csv.recover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import etl.jobs.jobproperties.JobProperties;

//takes as input a RECOVER Demographics file and outputs a csv which converts the data value columns into concept/answer pairs for each participant
//generates concepts for onstudy_infection, onstudy_infection_cnt, sex_at_birth, DOB, age_at_enrollment, enroll_zip_code, withdrawn, withdraw_date, enroll_date, deceased, deceased_date
public class DemographicsTransformation extends etl.jobs.Job {

private static String inputFile;
private static String outputFile;


    public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
			
			setLocalVariables(args, buildProperties(args));
			
		} catch (Exception e) {
			
			System.err.println("Error processing variables");
			
			System.err.println(e);
			
		}	

		try {
			execute();
		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
    private static void execute() throws IOException, Exception {
		

	}
	
	private static void setLocalVariables(String[] args, JobProperties buildProperties) throws Exception {
		for(String arg: args) {	
			if(arg.equalsIgnoreCase( "-inputfile" )){
				inputFile = checkPassedArgs(arg, args);
			}
            if(arg.equalsIgnoreCase( "-outputfile" )){
				outputFile = checkPassedArgs(arg, args);
			}
		}
	}
}
