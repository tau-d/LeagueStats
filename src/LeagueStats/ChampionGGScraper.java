package LeagueStats;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.LogFactory;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlDivision;
import com.gargoylesoftware.htmlunit.html.HtmlOption;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlSelect;

public class ChampionGGScraper {
	public static List<Pair<Long, Integer>> scrapePlayerPageForMatchIds(String playerName, Map<String, Integer> champNameToId) {
		System.out.println("Beginning scraping: " + playerName);
		
		final String url = "https://na.op.gg/summoner/userName=" + playerName;
		
		LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
		java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(Level.OFF);
		java.util.logging.Logger.getLogger("org.apache.commons.httpclient").setLevel(Level.OFF);
		
		final WebClient webClient = new WebClient();
        WebRequest request = null;
		try {
			request = new WebRequest(new URL(url));
		} catch (MalformedURLException e) {
			e.printStackTrace();
			webClient.close();
			return null;
		}

	    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.setJavaScriptTimeout(10000);
        webClient.getOptions().setCssEnabled(false);
        webClient.getOptions().setDownloadImages(false);
        webClient.getOptions().setTimeout(10000);

        HtmlPage page = null;
        try {
        	page = webClient.getPage(request);
			webClient.waitForBackgroundJavaScript(8000);
		} catch (FailingHttpStatusCodeException | IOException e) {
			e.printStackTrace();
			webClient.close();
			return null;
		}
        
        // select "Normal (Draft, Blind)" from dropdown menu
        System.out.println("Selecting Normals");
        List<?> queueTypeSelector = page.getByXPath( "//*[@id='SummonerLayoutContent']/div[1]/div[2]/div/div[1]/div/ul/li[4]/span/select");
        HtmlSelect select = (HtmlSelect) queueTypeSelector.get(0);
        HtmlOption option = select.getOptionByValue("normal");
        select.setSelectedAttribute(option, true);
        webClient.waitForBackgroundJavaScript(10000);
        
        // get "Show More" button that loads more matches
        System.out.println("Getting more matches...");
        List<?> buttonList = page.getByXPath("//div[@class='GameMoreButton Box']/a[not(@disabled)]");
        
        // keep clicking button to load more matches until there are no more to load
        int count = 0;
        while (!buttonList.isEmpty()) {
        	try {
        		HtmlAnchor button = (HtmlAnchor) buttonList.get(0);
        		System.out.println("Times clicked: " + ++count);
        		page = button.click();
				webClient.waitForBackgroundJavaScript(5000);
				buttonList = page.getByXPath( "//div[@class='GameMoreButton Box']/a[not(@disabled)]");
			} catch (IOException e) {
				e.printStackTrace();
				webClient.close();
				return null;
			}
        }
        System.out.println("Got all matches");

        // get GameItemList divs
        List<?> gameItemWraps = page.getByXPath("//div[@class='GameItemWrap']");
        System.out.println(gameItemWraps.size());
        
        // extract champion name and match id pairs
        List<Pair<Long, Integer>> matchIdChampNamePairs = new ArrayList<>();
        for (Object obj : gameItemWraps) {
        	HtmlDivision div = (HtmlDivision) obj;
        	
        	HtmlDivision champNameDiv = div.getFirstByXPath(".//div[@class='ChampionName']");
        	String champName = champNameDiv.asText();
        	
        	HtmlDivision gameItemDiv = (HtmlDivision) div.getFirstElementChild();
        	String s = gameItemDiv.getAttribute("data-game-id");
        	Long matchId = Long.parseLong(s);
        	
    		matchIdChampNamePairs.add(Pair.of(matchId, champNameToId.get(champName)));
        }
        
        webClient.close();
        
        return matchIdChampNamePairs;
	}
}
