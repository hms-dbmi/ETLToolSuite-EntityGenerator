package etl.jobs.csv;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class DbgapHierarchyGenerator extends Job {
	private static WebClient webClient;
	
	/**
	 * This class will generate a hierarchy csv from dbgaps website.
	 * it will scrape the variable tree explorer to build initial paths to be 
	 * used by the etl process.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			setVariables(args, buildProperties(args));
		} catch (Exception e) {
			System.err.println("Error processing variables");
			System.err.println(e);
		}
		
		try {
			execute();
		} catch (FailingHttpStatusCodeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}

	private static void execute() throws FailingHttpStatusCodeException, MalformedURLException, IOException {
		webClient = new WebClient(BrowserVersion.CHROME);
		webClient.getOptions().setCssEnabled(false);
		webClient.getOptions().setJavaScriptEnabled(true);
		webClient.waitForBackgroundJavaScript(10000);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.waitForBackgroundJavaScriptStartingBefore(10000);
		
		String studyID = "phs000286.v6.p2";
		
		String phv = "127982";
				
		String pht = "1921";
								
		String url = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/variable.cgi?" +
				"study_id=" + studyID + "&" +
				"phv=" + phv + "&" + 
				"phd=" + "&" +
				"pha=" + "&" +
				"pht=" + pht + "&" +
				"phvf=" + "&" + 
				"phdf=" + "&" + 
				"phaf=&" + 
				"phtf=&" + 
				"dssp=" + "3" + "&" +
				"consent=&" + 
				"temp=";
		
		HtmlPage page = webClient.getPage(url);
		
		String studyNodeXpath = "//div[contains(@class, 'studyNode')";
		
		List<Object> studyDiv = page.getByXPath(studyNodeXpath);
		
		for(Object o: studyDiv) {
			System.out.println(o);
		}
	}
	
	
}
