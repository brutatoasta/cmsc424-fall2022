import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;

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
	public static void executeNearestNeighbor(Connection connection) {
		/************* 
		 * Add your code to add a new column to the users table (set to null by default), calculate the nearest neighbor for each node (within first 5000), and write it back into the database for those users..
		 ************/
		addCol(connection);
		ResultSet rs = getInfo(connection);
		while (rs.next()) {
			String name = rs.
			
	}
	

        return;
	}
	public static Connection connectDB(){
		Connection connection = null;
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

		 // Set up the connection
		
		 try {
				 connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stackexchange","root", "root");
		 } catch (SQLException e) {
				 System.out.println("Connection Failed! Check output console");
				 e.printStackTrace();
				 return null;
		 }

		 if (connection != null) {
				 System.out.println("You made it, take control your database now!");
				 return connection;
		 } else {
				 System.out.println("Failed to make connection!");
				 return null;
		 }
	}
	public static void addCol(Connection connection){
		Statement stmt = null;
		String update = "ALTER TABLE users" +
						"ADD nearest_neighbor INTEGER;";
		try {
				stmt = connection.createStatement();
				stmt.executeUpdate(update);
				
				stmt.close();
		} catch (SQLException e ) {
				System.out.println(e);
		}
	}
	public static ResultSet getInfo(Connection connection){
		Statement stmt = null;
		ResultSet rs = null;
		String query = "select users.id, array_remove(array_agg(posts.tags), null) as arr" +
						"from users, posts " +
						"where users.id = posts.OwnerUserId and users.id < 5000 " +
						"group by users.id" +
						"having count(posts.tags) > 0;";
		try {
				stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE , ResultSet.CONCUR_UPDATABLE);
				rs = stmt.executeQuery(query);
				
		} catch (SQLException e ) {
				System.out.println(e);
		}
		return rs;
	}

	public static void main(String[] argv) {
		Connection connection = connectDB();
		executeNearestNeighbor(connection);
	}
}
