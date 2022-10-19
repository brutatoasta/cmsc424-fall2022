import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
		 // System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
		 try {
				 Class.forName("org.postgresql.Driver");
		 } catch (ClassNotFoundException e) {
				 System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				 e.printStackTrace();
				 return null;
		 }
		 // System.out.println("PostgreSQL JDBC Driver Registered!");

		 // Set up the conn
		
		 try {
				conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName,"root", "root");
		 } catch (SQLException e) {
				 System.out.println("Connection Failed! Check output console");
				 e.printStackTrace();
				 return null;
		 }

		 if (conn != null) {
				 // System.out.println("You made it, take control your database now!");
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
		ArrayList<String> joinablePairs = new ArrayList<>(); 
		DatabaseMetaData dbmd =null;
		try{
			dbmd = conn.getMetaData();
		}
		catch (SQLException e){
			e.printStackTrace();
		}
		System.out.println("### Tables in the Database");
		try{ 
			ResultSet resultSet = dbmd.getTables(null, null, null, new String[]{"TABLE"});
			while(resultSet.next()) { 
				/// get table name
				String tableName = resultSet.getString("TABLE_NAME"); 
				System.out.println("-- Table " + tableName.toUpperCase());
				
				//get attributes
				String attributeln = "Attributes: ";
				HashMap<String, String> attributeMap = new HashMap<>();
				ResultSet columns = dbmd.getColumns(null, null, tableName, null);

				while (columns.next()){
					String attribute = columns.getString("COLUMN_NAME");
					int dataTypeInt = columns.getInt("DATA_TYPE");
					String dataType = dataTypeName(dataTypeInt);
					attributeMap.put(attribute.toUpperCase(), dataType);					
				}

				ArrayList<String> orderedAttribute = new ArrayList<>(attributeMap.keySet());
				Collections.sort(orderedAttribute);
				for (String a : orderedAttribute){
					attributeln += a + " (" + attributeMap.get(a) + "), ";
				}
				System.out.println(attributeln.substring(0, attributeln.length() -2));

				// get primary key
				String pkLine = "Primary Key: ";
				ArrayList<String> pkLineList = new ArrayList<>();
				ResultSet pKey = dbmd.getPrimaryKeys(null, null, tableName);
				while(pKey.next()){
					String pKeyName = pKey.getString("COLUMN_NAME");
					pkLineList.add(pKeyName.toUpperCase());					
				}
				Collections.sort(pkLineList);
				for (String b : pkLineList ){
					pkLine +=  b + ", ";
				}
				System.out.println(pkLine.substring(0, pkLine.length() -2));
				
				//get Joinable pairs
				ResultSet fKeys = dbmd.getImportedKeys(null, null, tableName);		
				
				while (fKeys.next()){
					String pkTableName = fKeys.getString("PKTABLE_NAME").toUpperCase();
					String fkTableName = fKeys.getString("FKTABLE_NAME").toUpperCase();
					String pkColumnName = fKeys.getString("PKCOLUMN_NAME").toUpperCase();
					String fkColumnName = fKeys.getString("FKCOLUMN_NAME").toUpperCase();
					String joinablePairLine = String.format("%s can be joined %s on attributes %s and %s", pkTableName, fkTableName, pkColumnName, fkColumnName);
					joinablePairs.add(joinablePairLine);
				}		
				
			}
			
			System.out.println("\n### Joinable Pairs of Tables (based on Foreign Keys)");
			Collections.sort(joinablePairs);
			for (String jP : joinablePairs){
				System.out.println(jP);
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
