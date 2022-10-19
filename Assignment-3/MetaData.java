import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;

public class MetaData 
{
	static String dataTypeName(int i) {
		switch (i) {
			case java.sql.Types.INTEGER: return "Integer";
			case java.sql.Types.REAL: return "Real";
			case java.sql.Types.VARCHAR: return "Varchar";
			case java.sql.Types.TIMESTAMP: return "Timestamp";
			case java.sql.Types.DATE: return "Date";
		}
		return "Other";
	}
	public static Connection connectDB(String databaseName){
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
				String url = String.format("jdbc:postgresql://localhost:5432/%s","root", "root", databaseName);
				 conn = DriverManager.getConnection(url);
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
	public static void executeMetadata(String databaseName) {
		/************* 
		 * Add you code to connect to the database and print out the metadata for the database databaseName. 
		 ************/
		Connection conn = connectDB(databaseName);
		DatabaseMetaData dbmd =null;
		try{
			dbmd = conn.getMetaData();
		}
		catch (SQLException e){
			e.printStackTrace();
		}
		try{ 
			ResultSet resultSet = dbmd.getTables(null, null, null, new String[]{"TABLE"});
			while(resultSet.next()) { 
			  String tableName = resultSet.getString("TABLE_NAME"); 
			  String remarks = resultSet.getString("REMARKS"); 
			}
		  }
		  catch (SQLException e){
			e.printStackTrace();
		}


	}

	public static void main(String[] argv) {
		executeMetadata(argv[0]);
	}
}
