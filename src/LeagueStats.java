import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;


// TODO: cs/min, cs/gold/xp/etc at 10/20 min
// TODO: overall kda/winrate, playtime in different tabs?
// TODO: exclude remakes (matches with duration < 300 seconds)
// TODO: when updating, store set of matches and check if match has already been fetched to prevent repeats

public class LeagueStats {

	private static final int RANKED_SOLO_5x5 = 4;
	private static final int RANKED_PREMADE_5x5 = 6; // deprecated
	private static final int NORMAL_5x5_DRAFT = 14;
	private static final int RANKED_TEAM_5x5 = 42;
	private static final int TEAM_BUILDER_DRAFT_UNRANKED_5x5 = 400;
	private static final int TEAM_BUILDER_DRAFT_RANKED_5x5 = 410; // deprecated
	private static final int TEAM_BUILDER_RANKED_SOLO = 420;
	private static final int RANKED_FLEX_SR = 440;
	
	private String apiKey;
	private MySqlHelper helper;
	private Map<Long, JSONObject> cachedMatches;
	
	public LeagueStats() {
		try {
			setApiKey();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		helper = new MySqlHelper();
		cachedMatches = new HashMap<>();
	}
	
	private void setApiKey() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File("api_key")));
		apiKey = br.readLine();
		//if (apiKey.size() != 42)
		br.close();
	}
	
	private List<Pair<Long, String>> getPlayerIds() {
//		List<Pair<Long, String>> list = new ArrayList<>();
//		list.add(Pair.of(47940897L, "Praner"));
//		return list;
		return helper.getPlayer();
	}
	
	public void addAllNewPlayerMatches() {
		for (Pair<Long, String> player : getPlayerIds()) {
			addNewPlayerMatches(player.getLeft());
		}
	}
	
	private void addNewPlayerMatches(long accountId) {
		getMatchListAndAddNewMatches(accountId, helper.getMostRecentMatchTime(accountId));
	}
	
	private void getMatchListAndAddNewMatches(long accountId, long mostRecent) {
		String matchlistUrl = "https://na1.api.riotgames.com/lol/match/v3/matchlists/by-account/" + accountId + "/recent?api_key=" + apiKey;
		
		try {
            String matchListJsonString = IOUtils.toString(new URL(matchlistUrl), "utf-8");
            Thread.sleep(1000); // rate limited to 10 per 10 seconds
            JSONObject matchListJsonObject = (JSONObject) JSONValue.parseWithException(matchListJsonString);
            
            System.out.println(matchListJsonObject.toJSONString());
            
            JSONArray matchArray = (JSONArray) matchListJsonObject.get("matches");
            for (int i = 0; i < matchArray.size(); ++i) {
            	JSONObject match = (JSONObject) matchArray.get(i);
            	
                int queueId = ((Long) match.get("queue")).intValue();
                if (!isValidQueueType(queueId)) return; // skip non-normal games
                
                long timestamp = (long) match.get("timestamp");;
                if (timestamp <= mostRecent) return; // skip game that should already be stored
            	
            	long matchId = (long) match.get("gameId");
            	int champId = ((Long) match.get("champion")).intValue();
            	
            	addPlayerMatchStats(matchId, accountId, champId, mostRecent);
            	//System.out.printf("%d %d %d %d%n", matchId, champId, queue, timestamp);
            }
        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
	}
	
	private void addPlayerMatchStats(long matchId, long accountId, int champId, long timestampLowerBound) {
		try {
            JSONObject matchStatsJsonObject = cachedMatches.get(matchId); // get cached JSONObject if possible 
            if (matchStatsJsonObject == null) { // only call Riot API and sleep if match not cached
            	String matchStatsUrl = "https://na1.api.riotgames.com/lol/match/v3/matches/" + matchId + "?api_key=" + apiKey;
            	String matchStatsJsonString = IOUtils.toString(new URL(matchStatsUrl), "utf-8");
            	matchStatsJsonObject = (JSONObject) JSONValue.parseWithException(matchStatsJsonString);
            	cachedMatches.put(matchId, matchStatsJsonObject); // cache for later
            	Thread.sleep(1000); // rate limited to 10 per 10 seconds
            }     
            
            int queueId = ((Long) matchStatsJsonObject.get("queueId")).intValue();
            if (!isValidQueueType(queueId)) return; // skip non-normal games
            
            long timestamp = (long) matchStatsJsonObject.get("gameCreation");;
            if (timestamp <= timestampLowerBound) return; // skip game that should already be recorded
            
            JSONArray playersArray = (JSONArray) matchStatsJsonObject.get("participants");
            for (int i = 0; i < playersArray.size(); ++i) {
            	JSONObject player = (JSONObject) playersArray.get(i);
            	
            	int championId = ((Long) player.get("championId")).intValue();
            	if (champId != championId) {
            		continue;
            	} else {
            		// Stats
            		JSONObject stats = (JSONObject) player.get("stats");
            		int kills = ((Long) stats.get("kills")).intValue();
            		int deaths = ((Long) stats.get("deaths")).intValue();
            		int assists = ((Long) stats.get("assists")).intValue();
            		boolean win = (boolean) stats.get("win");
            		
            		// Timeline
            		JSONObject timeline = (JSONObject) player.get("timeline");
            		JSONObject goldPerMinDeltas = (JSONObject) timeline.get("goldPerMinDeltas");
            		JSONObject creepsPerMinDeltas = (JSONObject) timeline.get("creepsPerMinDeltas");
            		JSONObject xpPerMinDeltas = (JSONObject) timeline.get("xpPerMinDeltas");
            		
            		System.out.println(goldPerMinDeltas);
            		System.out.println(creepsPerMinDeltas);
            		System.out.println(xpPerMinDeltas);
            		
            		helper.insertPlayerMatch(matchId, accountId, champId, queueId, timestamp, kills, deaths, assists, win);
            		//System.out.printf("%d %d %d %d %d %d %d %d %b %n", matchId, accountId, champId, queue, timestamp, kills, deaths, assists, win);
            		break;
            	}
            }
        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
		
	}
	
	private boolean isValidQueueType(int queueTypeId) {
		if (queueTypeId == RANKED_SOLO_5x5 || 
			queueTypeId == RANKED_PREMADE_5x5 || 
			queueTypeId == NORMAL_5x5_DRAFT || 
			queueTypeId == RANKED_TEAM_5x5 ||
			queueTypeId == TEAM_BUILDER_DRAFT_UNRANKED_5x5 ||
			queueTypeId == TEAM_BUILDER_DRAFT_RANKED_5x5 ||
			queueTypeId == TEAM_BUILDER_RANKED_SOLO ||
			queueTypeId == RANKED_FLEX_SR) {
			return true;
		} else {
			return false;
		}
	}
	
	private void getPlayerChampKDAs(long accountId) {
		for (Pair<String, Double> p : helper.getPlayerChampKDAs(accountId)) {
			System.out.println("\t" + p);
		}
	}	
	
	
	// Database initialization methods
	private void initChampionsTable() {
		String champDataUrl = "https://na1.api.riotgames.com/lol/static-data/v3/champions?dataById=true&api_key=" + apiKey;
		
		try {
            String champDataJsonString = IOUtils.toString(new URL(champDataUrl), "utf-8");
            Thread.sleep(1000); // rate limited to 10 per 10 seconds
            JSONObject champDataJsonObject = (JSONObject) JSONValue.parseWithException(champDataJsonString);
            helper.initChampionTable((JSONObject) champDataJsonObject.get("data"));
		} catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
	}
	
	private void initPlayersTable() {
		long accountIds[] = 		{200933,		43795088,			47940897,	46656749,	46286606,	33699096};
		String summonerNames[] = 	{"ChickenC3",	"Bearer 0f Terror",	"Praner",	"aunvir",	"EndWolf",	"Aesmis"};
		
		for (int i = 0; i < accountIds.length; ++i) {
			helper.insertPlayer(accountIds[i], summonerNames[i]);
		}
	}
	
	private void initAllPlayerMatches() {
		for (Pair<Long, String> idNamePair : getPlayerIds()) {
			Map<String, Integer> champToId = getChampToIdMap();
			List<Pair<Long, Integer>> matchIdChampIdList = ChampionGGScraper.scrapePlayerPageForMatchIds(idNamePair.getRight(), champToId);
			for (Pair<Long, Integer> matchIdChampIdPair : matchIdChampIdList) {
				addPlayerMatchStats(matchIdChampIdPair.getLeft(), 
						idNamePair.getLeft(), 
						matchIdChampIdPair.getRight(), 
						Long.MIN_VALUE); // no time lower bound
			}
		}
	}
	
	private Map<String, Integer> getChampToIdMap() {
		return helper.getChampToIdMap();
	}
	
	
	// MAIN	FOR TESTING
	public static void main(String[] args) {
		LeagueStats ls = new LeagueStats();
//		ls.initChampions();
//		ls.initPlayers();
		
		ls.addAllNewPlayerMatches();
		
//		for (Pair<Long, String> player : ls.getPlayerIds()) {
//			System.out.println(player.getRight() + ": ");
//			ls.getPlayerChampKDAs(player.getLeft());
//		}
		
//		for (Pair<Long, Integer> pair : ChampionGGScraper.scrapePlayerPageForMatchIds("chickenc3", ls.getChampToIdMap())) {
//			System.out.println(pair.getLeft() + " " + pair.getRight());
//		}
		
		//ls.initAllPlayerMatches();
	}
}
