package LeagueStats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

public class MySqlHelper {
	
	protected static final String TABLE_PLAYERMATCHES = "playermatches";
	protected static final String COL_MATCH_ID = "matchId";
	protected static final String COL_QUEUE = "queue";
	protected static final String COL_TIMESTAMP = "timestamp";
	protected static final String COL_KILLS = "kills";
	protected static final String COL_DEATHS = "deaths";
	protected static final String COL_ASSISTS = "assists";
	protected static final String COL_WIN = "win";
	protected static final String COL_CS_0_TO_10 = "cs0to10";
	protected static final String COL_CS_10_TO_20 = "cs10to20";
	protected static final String COL_GOLD_0_TO_10 = "gold0to10";
	protected static final String COL_GOLD_10_TO_20 = "gold10to20";
	protected static final String COL_XP_0_TO_10 = "xp0to10";
	protected static final String COL_XP_10_TO_20 = "xp10to20";
	
	protected static final String TABLE_CHAMPIONS = "champions";
	protected static final String COL_CHAMPION_ID = "championId"; // shared with playermatches table
	protected static final String COL_CHAMPION_NAME = "championName";
	
	protected static final String TABLE_PLAYERS = "players";
	protected static final String COL_ACCOUNT_ID = "accountId"; // shared with playermatches table
	protected static final String COL_SUMMONER_NAME = "summonerName";
	
	
	private Connection conn;
	
	
	// Constructors
	public MySqlHelper(String url, String user, String pass) {
		setConnection(url, user, pass);
	}
	
	public MySqlHelper() {
		setConnection();
	}
	
	private void setConnection(String url, String user, String pass) {
		try {
			conn = DriverManager.getConnection(url, user, pass);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void setConnection() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File("mysql")));
			
			String url = br.readLine();
			String user = br.readLine();
			String pass = br.readLine();
			
			setConnection(url, user, pass);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.err.println("Failed to open mysql file");
			return;
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("mysql file formatted incorrectly");
		} finally {
			try {
				if (br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	// Methods
	public void insertPlayerMatch(RiotApiHelper.PlayerMatchStats pms) {
		if (conn == null) {
			throw new IllegalStateException("mysql connection is null");
		}
		
		PreparedStatement insertStmt = null;
		String insertStr = 
				"INSERT INTO " + TABLE_PLAYERMATCHES + " (" + 
					COL_ACCOUNT_ID +  "," + COL_MATCH_ID +  "," + COL_CHAMPION_ID +  "," + COL_QUEUE +  "," + 
					COL_TIMESTAMP +  "," + COL_KILLS +  "," + COL_DEATHS +  "," + COL_ASSISTS +  "," + COL_WIN + "," +
					COL_CS_0_TO_10 + "," + COL_CS_10_TO_20 + "," + 
					COL_GOLD_0_TO_10 + "," + COL_GOLD_10_TO_20 + "," + 
					COL_XP_0_TO_10 + "," + COL_XP_10_TO_20 + 
				") " + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		try {
			conn.setAutoCommit(false);
			insertStmt = conn.prepareStatement(insertStr);
			
			Integer index = 1;
			insertStmt.setLong(index++, pms.getAccountId());
			insertStmt.setLong(index++, pms.getMatchId());
			insertStmt.setInt(index++, pms.getChampionId());
			insertStmt.setInt(index++, pms.getQueueId());
			insertStmt.setLong(index++, pms.getTimestamp());
			insertStmt.setInt(index++, pms.getKills());
			insertStmt.setInt(index++, pms.getDeaths());
			insertStmt.setInt(index++, pms.getAssists());
			insertStmt.setBoolean(index++, pms.getWin());
			setFloatOrNull(insertStmt, index++, pms.getCs0to10());
			setFloatOrNull(insertStmt, index++, pms.getCs10to20());
			setFloatOrNull(insertStmt, index++, pms.getGold0to10());
			setFloatOrNull(insertStmt, index++, pms.getGold10to20());
			setFloatOrNull(insertStmt, index++, pms.getXp0to10());
			setFloatOrNull(insertStmt, index++, pms.getXp10to20());
			
			System.out.println(insertStmt);
			insertStmt.execute();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public List<Pair<String, Double>> getPlayerChampKDAs(Long accountId) {
		PreparedStatement selectKDA = null;
		final String k = "totalKills";
		final String d = "totalDeaths";
		final String a = "totalAssists";
		final String kda = "KDA";
		final String pm = "pm";
		final String c = "c";
		final String agg = "aggregates";
		
		String selectStr = 
				"SELECT " + COL_CHAMPION_NAME + ", ((" + k + " + " + a + ") / " + d + ") " + kda + " " + 
				"FROM " +  
					"(SELECT " + COL_CHAMPION_NAME + ", SUM(" + COL_KILLS + ") " + k + ", SUM(" + COL_DEATHS + ") " + d + ", SUM(" + COL_ASSISTS + ") " + a + " " +
					"FROM " + TABLE_PLAYERMATCHES + " " + pm + ", " + TABLE_CHAMPIONS + " " + c + " " + 
					"WHERE " + pm + "." + COL_ACCOUNT_ID + " = ? AND " + pm + "." + COL_CHAMPION_ID + " = " + c + "." + COL_CHAMPION_ID + " " +
					"GROUP BY " + pm + "." + COL_ACCOUNT_ID + ", " + pm + "." + COL_CHAMPION_ID + ") " + agg;
		
		List<Pair<String, Double>> ret = new ArrayList<>();
		try {
			selectKDA = conn.prepareStatement(selectStr);
			selectKDA.setLong(1, accountId);
			
			ResultSet rs = selectKDA.executeQuery();
			while (rs.next()) {
				ret.add(Pair.of(rs.getString(COL_CHAMPION_NAME), rs.getDouble(kda)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return ret;
	}
		
	public void initChampionTable(JSONObject data) {
		for (Object entry : data.entrySet()) {
			Entry<Object, Object> champ = (Entry<Object, Object>) entry;
			JSONObject obj = (JSONObject) champ.getValue();
			
			Integer champId = ((Long) obj.get("id")).intValue();
			String name = (String) obj.get("name");
			
			PreparedStatement insertChamp = null;
			String insertStr = 
					"INSERT INTO " + TABLE_CHAMPIONS + 
					" (" + COL_CHAMPION_ID +  "," + COL_CHAMPION_NAME + ") " + 
					"VALUES(?,?)";
			
			try {
				conn.setAutoCommit(false);
				insertChamp = conn.prepareStatement(insertStr);
				insertChamp.setInt(1, champId);
				insertChamp.setString(2, name);
				insertChamp.execute();
				conn.commit();
			} catch (MySQLIntegrityConstraintViolationException e) {
				System.err.println(e.toString());
			} catch (SQLException e) {
				e.printStackTrace();
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public void insertPlayer(Long accountId, String summonerName) {
		PreparedStatement insertPlayer= null;
		String insertStr = 
				"INSERT INTO " + TABLE_PLAYERS + 
				" (" + COL_ACCOUNT_ID +  "," + COL_SUMMONER_NAME + ") " + 
				"VALUES(?,?)";
		
		try {
			conn.setAutoCommit(false);
			insertPlayer = conn.prepareStatement(insertStr);
			insertPlayer.setLong(1, accountId);
			insertPlayer.setString(2, summonerName);
			insertPlayer.execute();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	public List<Pair<Long, String>> getPlayers() {
		List<Pair<Long, String>> ret = new ArrayList<>();
		
		Statement stmt = null;
		String selectStr = "SELECT " + COL_ACCOUNT_ID + ", " + COL_SUMMONER_NAME + " FROM " + TABLE_PLAYERS;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(selectStr);			
			
			while (rs.next()) {
				ret.add(Pair.of(rs.getLong(COL_ACCOUNT_ID), rs.getString(COL_SUMMONER_NAME)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return ret;
	}

	public Long getMostRecentMatchTime(Long accountId) {
		PreparedStatement selectTimeStmt = null;
		String selectStr = 
				"SELECT " + COL_TIMESTAMP + " " +
				"FROM " + TABLE_PLAYERMATCHES + " " + 
				"WHERE " + COL_ACCOUNT_ID + " = ? " + 
				"ORDER BY " + COL_TIMESTAMP + " DESC " + 
				"LIMIT 1";
		
		try {
			selectTimeStmt = conn.prepareStatement(selectStr);
			selectTimeStmt.setLong(1, accountId);
			
			ResultSet rs = selectTimeStmt.executeQuery();
			if (rs.next()) {
				return rs.getLong(COL_TIMESTAMP);
			} else {
				System.err.println("No matches stored for player: " + accountId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return -1L;
	}
	
	public Map<String, Integer> getChampToIdMap() {
		Map<String, Integer> champNameToId = new HashMap<>();
		
		Statement selectChamps = null;
		String selectStr = "SELECT " + COL_CHAMPION_ID  + ", " + COL_CHAMPION_NAME + " FROM " + TABLE_CHAMPIONS;
		
		try {
			selectChamps = conn.createStatement();
			ResultSet rs = selectChamps.executeQuery(selectStr);
			
			while (rs.next()) {
				champNameToId.put(rs.getString(COL_CHAMPION_NAME), rs.getInt(COL_CHAMPION_ID));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return champNameToId;
	}
	
	public static void setFloatOrNull(PreparedStatement ps, int index, Float value) throws SQLException {
	    if (value == null) {
	        ps.setNull(index, Types.FLOAT);
	    } else {
	        ps.setFloat(index, value);
	    }
	}
	
	// Updating rows
	private void updatePlayerMatches() {
		ResultSet allMatches = getAllPlayerMatches();
		RiotApiHelper helper = null;
		try {
			helper = new RiotApiHelper();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Failed to update player matches");
			return;
		}
		
		PreparedStatement updatePlayerMatch = null;
		String updateStr = 
				"UPDATE " + TABLE_PLAYERMATCHES + " SET " +
				COL_CS_0_TO_10 + " = ?, " + COL_CS_10_TO_20 + " = ?, " +
				COL_GOLD_0_TO_10 + " = ?, " + COL_GOLD_10_TO_20 + " = ?, " +
				COL_XP_0_TO_10 + " = ?, " + COL_XP_10_TO_20 + " = ? " +
				"WHERE " + COL_MATCH_ID + " = ? AND " + COL_ACCOUNT_ID + " = ?";
		try {
			updatePlayerMatch = conn.prepareStatement(updateStr);
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
		
		try {
			while (allMatches.next()) {
				Long currMatchId = allMatches.getLong(COL_MATCH_ID);
				JSONObject matchJsonObj = helper.getMatch(currMatchId);
				
				Long currAccId = allMatches.getLong(COL_ACCOUNT_ID);
				Integer currChampId = allMatches.getInt(COL_CHAMPION_ID);
				
				RiotApiHelper.PlayerMatchStats pms = 
						new RiotApiHelper.PlayerMatchStats(currAccId, currChampId, matchJsonObj);
				
				try {
					conn.setAutoCommit(false);
					
					int index = 1;
					setFloatOrNull(updatePlayerMatch, index++, pms.getCs0to10());
					setFloatOrNull(updatePlayerMatch, index++, pms.getCs10to20());
					setFloatOrNull(updatePlayerMatch, index++, pms.getGold0to10());
					setFloatOrNull(updatePlayerMatch, index++, pms.getGold10to20());
					setFloatOrNull(updatePlayerMatch, index++, pms.getXp0to10());
					setFloatOrNull(updatePlayerMatch, index++, pms.getXp10to20());;
					updatePlayerMatch.setLong(index++, currMatchId);
					updatePlayerMatch.setLong(index++, currAccId);
					updatePlayerMatch.execute();
					
					System.out.println(updatePlayerMatch);
					conn.commit();
				} catch (SQLException e) {
					e.printStackTrace();
					try {
						conn.rollback();
					} catch (SQLException ex) {
						ex.printStackTrace();
					}
				}			
			}
		} catch (SQLException | IOException | ParseException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private ResultSet getAllPlayerMatches() {
		Statement stmt = null;
		String selectStr = "SELECT * FROM " + TABLE_PLAYERMATCHES + " ORDER BY " + COL_MATCH_ID + " DESC";
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(selectStr);			
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return null;
	}
	
	// MAIN FOR TESTING
	public static void main(String[] args) {
		MySqlHelper h = new MySqlHelper();
		
		System.out.println("COMPLETE");
	}

	
}
