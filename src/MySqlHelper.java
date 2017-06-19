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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;

public class MySqlHelper {
	
	protected static final String TABLE_PLAYERMATCHES = "playermatches";
	protected static final String COL_MATCH_ID = "matchId";
	protected static final String COL_QUEUE = "queue";
	protected static final String COL_TIMESTAMP = "timestamp";
	protected static final String COL_KILLS = "kills";
	protected static final String COL_DEATHS = "deaths";
	protected static final String COL_ASSISTS = "assists";
	protected static final String COL_WIN = "win";
	
	protected static final String TABLE_CHAMPIONS = "champions";
	protected static final String COL_CHAMPION_ID = "championId"; // shared with playermatches table
	protected static final String COL_CHAMPION_NAME = "championName";
	
	protected static final String TABLE_PLAYERS = "players";
	protected static final String COL_ACCOUNT_ID = "accountId"; // shared with playermatches table
	protected static final String COL_SUMMONER_NAME = "summonerName";
	
	
	private Connection conn;
	
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

	public void insertPlayerMatch(long accountId, long matchId, int champId, int queue, long timestamp, int kills, int deaths, int assists, boolean win) {
		if (conn == null) {
			throw new IllegalStateException("mysql connection is null");
		}
		
		PreparedStatement insertStmt = null;
		String insertStr = 
				"INSERT INTO " + TABLE_PLAYERMATCHES + " (" + 
				COL_ACCOUNT_ID +  "," + COL_MATCH_ID +  "," + COL_CHAMPION_ID +  "," + 
				COL_QUEUE +  "," + COL_TIMESTAMP +  "," + COL_KILLS +  "," + COL_DEATHS +  "," + 
				COL_ASSISTS +  "," + COL_WIN + ") " + 
				"VALUES(?,?,?,?,?,?,?,?,?)";
		
		try {
			conn.setAutoCommit(false);
			insertStmt = conn.prepareStatement(insertStr);
			insertStmt.setLong(1, matchId);
			insertStmt.setLong(2, accountId);
			insertStmt.setInt(3, champId);
			insertStmt.setInt(4, queue);
			insertStmt.setLong(5, timestamp);
			insertStmt.setInt(6, kills);
			insertStmt.setInt(7, deaths);
			insertStmt.setInt(8, assists);
			insertStmt.setBoolean(9, win);
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

	public List<Pair<String, Double>> getPlayerChampKDAs(long accountId) {
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
	
	public void getPlayerChampWinRate(long accountId) {
		
	}
	
	public void initChampionTable(JSONObject data) {
		for (Object entry : data.entrySet()) {
			Entry<Object, Object> champ = (Entry<Object, Object>) entry;
			JSONObject obj = (JSONObject) champ.getValue();
			
			int champId = ((Long) obj.get("id")).intValue();
			String name = (String) obj.get("name");
			
			PreparedStatement insertChamp = null;
			String insertStr = 
					"INSERT INTO " + TABLE_CHAMPIONS + 
					" (" + COL_CHAMPION_ID +  "," + COL_CHAMPION_NAME + ") " + 
					"VALUES(?,?)";
			
			try {
				conn.setAutoCommit(false);
				insertChamp = conn.prepareStatement(insertStr);
				insertChamp.setLong(1, champId);
				insertChamp.setString(2, name);
				insertChamp.execute();
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
	}

	public void insertPlayer(long accountId, String summonerName) {
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

	public List<Pair<Long, String>> getPlayer() {
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

	public long getMostRecentMatchTime(long accountId) {
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
	
}
