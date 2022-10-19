import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class NearestNeighbor 
{
	static double jaccard(HashSet<String> s1, HashSet<String> s2) {
		int total_size = s1.size() + s2.size();
		int i_size = 0;
		for(String s: s1) {
			if (s2.contains(s))
				i_size++;
		}
		return ((double) i_size)/(total_size - i_size);
	}
	public static void executeNearestNeighbor(Connection conn) {
		/************* 
		 * Add your code to add a new column to the users table (set to null by default), calculate the nearest neighbor for each node (within first 5000), and write it back into the database for those users..
		 ************/
		addCol(conn);
		HashMap<Integer, Integer> jaccards = getInfo(conn);
		String update = "update users set nearest_neighbor= ? where id= ?";
		PreparedStatement preparedStatement = null;
	
		try {
			preparedStatement = conn.prepareStatement(update);
			for (Map.Entry<Integer, Integer> entry : jaccards.entrySet()) {
				int userid = entry.getKey();
				int nbr = entry.getValue();
				System.out.println("userid: "+ userid + " nbr: " + nbr + "\n");
				preparedStatement.setInt(1, nbr);
				preparedStatement.setInt(2, userid);
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			preparedStatement.close();
		} catch (SQLException e ) {
				System.out.println("jaccards not updated!\n");
				System.out.println(e);
		}

	}
	public static Connection connectDB(){
		Connection conn = null;
		 // Load the PostgreSQL JDBC Driver
		 System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
		 try {
				 Class.forName("org.postgresql.Driver");
		 } catch (ClassNotFoundException e) {
				 System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				 e.printStackTrace();
				 return null;
		 }
		 System.out.println("PostgreSQL JDBC Driver Registered!");

		 // Set up the conn
		
		 try {
				 conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stackexchange","root", "root");
		 } catch (SQLException e) {
				 System.out.println("Connection Failed! Check output console");
				 e.printStackTrace();
				 return null;
		 }

		 if (conn != null) {
				 System.out.println("You made it, take control your database now!");
				 return conn;
		 } else {
				 System.out.println("Failed to make conn!");
				 return null;
		 }
	}
	
	public static void dropCol(Connection conn){
		Statement stmt = null;
		String update = "alter table users " +
						"drop column nearest_neighbor;";
		try {
				stmt = conn.createStatement();
				stmt.executeUpdate(update);
				
				stmt.close();
		} catch (SQLException e ) {
				System.out.println("column not added!\n");
				System.out.println(e);
		}
	}
	public static void addCol(Connection conn){
		Statement stmt = null;
		String update = "alter table users " +
						"add column nearest_neighbor int default null;";
		try {
				stmt = conn.createStatement();
				stmt.executeUpdate(update);
				
				stmt.close();
		} catch (SQLException e ) {
				System.out.println("column not added!\n");
				System.out.println(e);
		}
	}
	public static HashMap<Integer, Integer> getInfo(Connection conn){
		Statement stmt = null;
		
		ArrayList<Integer> userids = new ArrayList<>();
		ArrayList<HashSet<String>> sets = new ArrayList<>();
		HashMap<Integer, Integer> jaccards = new HashMap<>();
		String query = "select users.id, array_remove(array_agg(posts.tags), null) as arr " +
						"from users, posts " +
						"where users.id = posts.OwnerUserId and users.id < 5000 " +
						"group by users.id " +
						"having count(posts.tags) > 0;";
		try {
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(query);
				
				// for each row in rs, populate lists of userids and sets
				while(rs.next()){
					//get userid
					int userid = rs.getInt("id");

					// get set of tags in each row
					HashSet<String> set = new HashSet<>();
					String tags = rs.getString("arr"); // error
					
					tags = tags.replaceAll("[><{},]", " "); // remove the tags
					
					
					String[] tags_arr = tags.split(" ");

					for (String tag : tags_arr){
						set.add(tag);
					}
					// for (String tag : set){
					// 	System.out.println(tag);
					// }
					// add to ArrayLists
					sets.add(set);
					userids.add(userid);
				}
				
				// calculate the jaccard coefficient, and update jaccards array
				int num_userids = userids.size();
				for (int i = 0; i < num_userids; i++){
					// for each user
					int me_userid = userids.get(i);
					HashSet<String> me_set = sets.get(i);
					// keep state of best jaccard coeff, and with who
					int best_friend = 0;
					Double best_friend_coeff = 0.0;

					//compare against every other users tags
					for (int j = 0; j < num_userids; j++ ){
						if (i == j){
							continue;
						}
						int you_userid = userids.get(j);
						HashSet<String> you_set = sets.get(j);
						Double newDouble = jaccard(me_set, you_set);

						if (newDouble > best_friend_coeff){
							best_friend_coeff = newDouble;
							best_friend = you_userid;
						}
						else if( newDouble == best_friend_coeff){
							// use the lowest userid
							if(you_userid < best_friend){ // if new userid is lower, update
								best_friend = you_userid;
							}
							// else leave alone
						}
					}
					// best friend is determined, add to jaccards
					jaccards.put(me_userid, best_friend);

				}
				stmt.close();
				
		} catch (SQLException e ) {
			System.out.println("getInfo: jaccards not made!\n");
				System.out.println(e);
		}
		return jaccards;
	}
	public static void check(Connection conn){
		Statement stmt = null;
		String query = "select * from users order by id limit 10;";
		try {
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(query);
				ResultSetMetaData rsmd = rs.getMetaData();
				int colNum = rsmd.getColumnCount();
				while (rs.next()){
					for(int i = 1 ; i <= colNum; i++){

						System.out.print(rs.getString(i) + " "); //Print one element of a row
				  
				  }
				  
					System.out.println();//Move to the next line to print the next row. 
				}
				stmt.close();
		} catch (SQLException e ) {
				System.out.println(e);
		}

	}
	public static void main(String[] argv) {
		Connection conn = connectDB();
		dropCol(conn);
		executeNearestNeighbor(conn);
		check(conn);
	}
}
