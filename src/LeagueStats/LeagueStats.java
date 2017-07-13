package LeagueStats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;


// TODO: overall kda/winrate, playtime in different tabs?
// TODO: exclude remakes (matches with duration < 300 seconds)


public class LeagueStats {
	
	private static final Set<Integer> VALID_QUEUE_TYPES = new HashSet<>();
	
	{
		final int RANKED_SOLO_5x5 = 4;
		final int RANKED_PREMADE_5x5 = 6; // deprecated
		final int NORMAL_5x5_DRAFT = 14;
		final int RANKED_TEAM_5x5 = 42;
		final int TEAM_BUILDER_DRAFT_UNRANKED_5x5 = 400;
		final int TEAM_BUILDER_DRAFT_RANKED_5x5 = 410; // deprecated
		final int TEAM_BUILDER_RANKED_SOLO = 420;
		final int RANKED_FLEX_SR = 440;
		
		VALID_QUEUE_TYPES.add(RANKED_SOLO_5x5);
		VALID_QUEUE_TYPES.add(RANKED_PREMADE_5x5);
		VALID_QUEUE_TYPES.add(NORMAL_5x5_DRAFT);
		VALID_QUEUE_TYPES.add(RANKED_TEAM_5x5);
		VALID_QUEUE_TYPES.add(TEAM_BUILDER_DRAFT_UNRANKED_5x5);
		VALID_QUEUE_TYPES.add(TEAM_BUILDER_DRAFT_RANKED_5x5);
		VALID_QUEUE_TYPES.add(TEAM_BUILDER_RANKED_SOLO);
		VALID_QUEUE_TYPES.add(RANKED_FLEX_SR);
	}
	
	private MySqlHelper mySqlHelper = new MySqlHelper();
	private RiotApiHelper riotHelper;
	
	public LeagueStats() {
		try {
			String apiKey = getApiKey();
			riotHelper = new RiotApiHelper(apiKey);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private String getApiKey() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File("api_key")));
		String apiKey = br.readLine();
		//if (apiKey.size() != 42)
		br.close();
		
		return apiKey;
	}
	
	private List<Pair<Long, String>> getPlayerIds() {
//		List<Pair<Long, String>> list = new ArrayList<>();
//		list.add(Pair.of(47940897L, "Praner"));
//		return list;
		return mySqlHelper.getPlayers();
	}
	
	public void addAllNewPlayerMatches() {
		for (Pair<Long, String> player : getPlayerIds()) {
			addNewPlayerMatches(player.getLeft());
		}
	}
	
	private void addNewPlayerMatches(Long accountId) {
		getMatchListAndAddNewMatches(accountId, mySqlHelper.getMostRecentMatchTime(accountId));
	}
	
	private void getMatchListAndAddNewMatches(Long accountId, Long mostRecent) {
		try {
			JSONObject matchListJsonObject = riotHelper.getPlayerMatchList(accountId);
            
            System.out.println(matchListJsonObject.toJSONString());
            
            JSONArray matchArray = (JSONArray) matchListJsonObject.get("matches");
            for (int i = 0; i < matchArray.size(); ++i) {
            	JSONObject match = (JSONObject) matchArray.get(i);
            	
            	Long timestamp = (Long) match.get("timestamp");;
                if (timestamp <= mostRecent) return; // skip game that should already be stored, remaining matches should also be stored already
            	
            	Integer queueId = RiotApiHelper.parseInt(match, "queue");
                if (!isValidQueueType(queueId)) continue; // skip non-standard games
                
            	Long matchId = (Long) match.get("gameId");
            	Integer champId = RiotApiHelper.parseInt(match, "champion");
            	
            	addPlayerMatchStats(matchId, accountId, champId, mostRecent);
            	//System.out.printf("%d %d %d %d%n", matchId, champId, queue, timestamp);
            }
        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
	}
	
	private void addPlayerMatchStats(Long matchId, Long accountId, Integer champId, Long timestampLowerBound) {
		try {
            JSONObject matchStatsJsonObject = riotHelper.getMatch(matchId); 
            
            RiotApiHelper.PlayerMatchStats pms = new RiotApiHelper.PlayerMatchStats(accountId, champId, matchStatsJsonObject);
            
            if (!isValidQueueType(pms.getQueueId())) return; // skip non-standard games
            if (pms.getTimestamp() <= timestampLowerBound) return; // skip game that should already be recorded
            
    		mySqlHelper.insertPlayerMatch(pms);
        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
		
	}
	
	private boolean isValidQueueType(Integer queueTypeId) {
		return VALID_QUEUE_TYPES.contains(queueTypeId);
	}
	
	
	// Database initialization methods
	protected void initChampionsTable() {
		JSONObject champDataJsonObject;
		try {
			champDataJsonObject = riotHelper.getChampions();
			mySqlHelper.initChampionTable((JSONObject) champDataJsonObject.get("data"));
		} catch (IOException | ParseException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	private void initPlayersTable() {
		long accountIds[] = 		{200933,		43795088,			47940897,	46656749,	46286606,	33699096};
		String summonerNames[] = 	{"ChickenC3",	"Bearer 0f Terror",	"Praner",	"aunvir",	"EndWolf",	"Aesmis"};
		
		for (int i = 0; i < accountIds.length; ++i) {
			mySqlHelper.insertPlayer(accountIds[i], summonerNames[i]);
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
		return mySqlHelper.getChampToIdMap();
	}
	
	
	// TESTING
	private void addAllRecentPlayerMatches() {
		for (Pair<Long, String> player : getPlayerIds()) {
			getMatchListAndAddNewMatches(player.getLeft(), 0L);
		}
		System.out.println("COMPLETE");
	}
	
	private void getPlayerChampKDAs(Long accountId) {
		for (Pair<String, Double> p : mySqlHelper.getPlayerChampKDAs(accountId)) {
			System.out.println("\t" + p);
		}
	}
	
	
	// MAIN	FOR TESTING
	public static void main(String[] args) {
		LeagueStats ls = new LeagueStats();
//		ls.initChampions();
//		ls.initPlayers();
		
		ls.addAllRecentPlayerMatches();
//		ls.initChampionsTable();
		
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
