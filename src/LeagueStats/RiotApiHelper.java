package LeagueStats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

public class RiotApiHelper {

	private static final long SLEEP_TIME = 1000 + 100; // 1 call per second plus some wiggle room 
	
	private String apiKey;
	private Map<Long, JSONObject> cachedMatches = new HashMap<>();
	
	public RiotApiHelper(String apiKey) {
		this.apiKey = apiKey;
	}
	
	public RiotApiHelper() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File("api_key")));
		apiKey = br.readLine();
		//if (apiKey.size() != 42)
		br.close();
	}
	
	public JSONObject getPlayerMatchList(Long accountId) throws MalformedURLException, IOException, ParseException, InterruptedException {
		String matchlistUrl = "https://na1.api.riotgames.com/lol/match/v3/matchlists/by-account/" + accountId + "/recent?api_key=" + apiKey;
		String matchListJsonString = IOUtils.toString(new URL(matchlistUrl), "utf-8");
		JSONObject matchListJsonObject = (JSONObject) JSONValue.parseWithException(matchListJsonString);
		Thread.sleep(SLEEP_TIME);
		
        return matchListJsonObject;
	}
	
	public JSONObject getMatch(Long matchId) throws MalformedURLException, IOException, ParseException, InterruptedException {
		JSONObject matchStatsJsonObject = cachedMatches.get(matchId);
		if (matchStatsJsonObject == null) { // only use Riot API and sleep if match not cached
			String matchStatsUrl = "https://na1.api.riotgames.com/lol/match/v3/matches/" + matchId + "?api_key=" + apiKey;
			String matchStatsJsonString = IOUtils.toString(new URL(matchStatsUrl), "utf-8");
			matchStatsJsonObject = (JSONObject) JSONValue.parseWithException(matchStatsJsonString);
			cachedMatches.put(matchId, matchStatsJsonObject); // cache for later
	    	Thread.sleep(SLEEP_TIME); // rate limited to 10 per 10 seconds
		}
    	return matchStatsJsonObject;
	}
	
	public JSONObject getChampions() throws MalformedURLException, IOException, ParseException, InterruptedException {
		String champDataUrl = "https://na1.api.riotgames.com/lol/static-data/v3/champions?dataById=true&api_key=" + apiKey;
        String champDataJsonString = IOUtils.toString(new URL(champDataUrl), "utf-8");
        JSONObject champDataJsonObject = (JSONObject) JSONValue.parseWithException(champDataJsonString);
        Thread.sleep(SLEEP_TIME); // rate limited to 10 per 10 seconds
		
        return champDataJsonObject;
	}
	
	protected static Float parseFloat(JSONObject jsonObj, String key) {
		if (jsonObj == null) return null;
		Object obj = jsonObj.get(key);
		if (obj == null) return null;
		else return ((Double) obj).floatValue();
	}
	
	protected static Integer parseInt(JSONObject jsonObj, String key) {
		if (jsonObj == null) return null;
		Object obj = jsonObj.get(key);
		if (obj == null) return null;
		else return ((Long) obj).intValue();
	}
	
	protected static class PlayerMatchStats {
		private Long accountId;
		private Long matchId;
		private Integer championId;
		private Integer queueId;
		private Long timestamp;
		private Integer kills;
		private Integer deaths;
		private Integer assists;
		private Boolean win;
		private Float cs0to10;
		private Float cs10to20;
		private Float gold0to10;
		private Float gold10to20;
		private Float xp0to10;
		private Float xp10to20;
		
		protected PlayerMatchStats(Long accountId, Integer championId, JSONObject matchJsonObj) {
			this.accountId = accountId;
			this.championId = championId;
			
			queueId = RiotApiHelper.parseInt(matchJsonObj, "queueId");
            timestamp = (Long) matchJsonObj.get("gameCreation");
            matchId = (Long) matchJsonObj.get("gameId");
            
            JSONArray playersArray = (JSONArray) matchJsonObj.get("participants");
            for (int i = 0; i < playersArray.size(); ++i) {
            	JSONObject player = (JSONObject) playersArray.get(i);
            	
            	Integer currChampionId = RiotApiHelper.parseInt(player, "championId");
            	if (!currChampionId.equals(this.championId)) {
            		continue;
            	} else {
            		// Stats
            		JSONObject stats = (JSONObject) player.get("stats");
            		kills = RiotApiHelper.parseInt(stats, "kills");
            		deaths = RiotApiHelper.parseInt(stats, "deaths");
            		assists = RiotApiHelper.parseInt(stats, "assists");
            		win = (Boolean) stats.get("win");
            		
            		// Timeline
            		JSONObject timeline = (JSONObject) player.get("timeline");
            		JSONObject creepsPerMinDeltas = (JSONObject) timeline.get("creepsPerMinDeltas");
            		JSONObject goldPerMinDeltas = (JSONObject) timeline.get("goldPerMinDeltas");
            		JSONObject xpPerMinDeltas = (JSONObject) timeline.get("xpPerMinDeltas");
            		
            		final String zeroToTen = "0-10";
            		final String tenToTwenty = "10-20";
            		
            		cs0to10 = RiotApiHelper.parseFloat(creepsPerMinDeltas, zeroToTen);
            		cs10to20 = RiotApiHelper.parseFloat(creepsPerMinDeltas, tenToTwenty);
            		gold0to10 = RiotApiHelper.parseFloat(goldPerMinDeltas, zeroToTen);
            		gold10to20 = RiotApiHelper.parseFloat(goldPerMinDeltas, tenToTwenty);
            		xp0to10 = RiotApiHelper.parseFloat(xpPerMinDeltas, zeroToTen);
            		xp10to20 = RiotApiHelper.parseFloat(xpPerMinDeltas, tenToTwenty);
            		
            		break;
            	}
            }
		}

		public Long getAccountId() {
			return accountId;
		}

		public Long getMatchId() {
			return matchId;
		}

		public Integer getChampionId() {
			return championId;
		}

		public Integer getQueueId() {
			return queueId;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public Integer getKills() {
			return kills;
		}

		public Integer getDeaths() {
			return deaths;
		}

		public Integer getAssists() {
			return assists;
		}

		public Boolean getWin() {
			return win;
		}

		public Float getCs0to10() {
			return cs0to10;
		}

		public Float getCs10to20() {
			return cs10to20;
		}

		public Float getGold0to10() {
			return gold0to10;
		}

		public Float getGold10to20() {
			return gold10to20;
		}

		public Float getXp0to10() {
			return xp0to10;
		}

		public Float getXp10to20() {
			return xp10to20;
		}
	}
}
