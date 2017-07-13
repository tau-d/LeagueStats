package LeagueStats;

public class UpdateLeagueStatsDatabase {

	public static void main(String[] args) {
		LeagueStats ls = new LeagueStats();
		ls.initChampionsTable(); // make sure new champions are in the database
		ls.addAllNewPlayerMatches();
	}

}
