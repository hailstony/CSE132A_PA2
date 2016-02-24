import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PA2{
	public static void main(String[] args){
		Connection conn = null;
		try{
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");
 			System.out.println("Opened database successfully.");
 			
 			Statement statement = conn.createStatement();
 			ResultSet set = null;
 			
 			statement.executeUpdate("DROP TABLE IF EXISTS Connected;");
 			statement.executeUpdate(
 			"CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
			//statement.executeUpdate("INSERT INTO Connected VALUES('F1','L1'),('F2','L2');");
			
			//create a temporary table for usage
			statement.executeUpdate("DROP TABLE IF EXISTS Temp;");
			statement.executeUpdate(
			"CREATE TABLE Temp(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
			//create table result, might contain duplicates
			statement.executeUpdate("DROP TABLE IF EXISTS Result;");
			statement.executeUpdate(
			"CREATE TABLE Result(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
			
			//initialize result and temp to have all the direct flight with 0 stop
			statement.executeUpdate("INSERT INTO Result SELECT *," + 0 + " FROM Flight;");
			
			
			//update the table 1 stop at a time
			int stop = 1;
			while(true){
				statement.executeUpdate("INSERT INTO Temp SELECT * FROM Result;");
				set = statement.executeQuery("SELECT COUNT(*) FROM Result;");
				int setSize = set.getInt(1);
				statement.executeUpdate(
				"INSERT INTO Result "+
				"SELECT t.Airline, f.Origin, t.Destination, "+stop+
				" FROM Temp t, Flight f "+
				"WHERE f.Airline=t.Airline AND "+
				"(f.Destination=t.Origin AND f.Origin<>t.Destination) "+
				"AND t.Stops="+(stop-1)+
				" AND NOT EXISTS(SELECT * FROM Result r "+
								"WHERE r.Airline=t.Airline AND r.Origin=f.Origin AND r.Destination=t.Destination);");
				statement.executeUpdate(
				"INSERT INTO Result "+
				"SELECT t.Airline, t.Origin, f.Destination, "+stop+
				" FROM Temp t, Flight f "+
				"WHERE f.Airline=t.Airline AND "+
				"(f.Destination<>t.Origin AND f.Origin=t.Destination) "+
				"AND t.Stops="+(stop-1)+
				" AND NOT EXISTS(SELECT * FROM Result r "+
								"WHERE r.Airline=t.Airline AND r.Origin=t.Origin AND r.Destination=f.Destination);");
				set = statement.executeQuery("SELECT COUNT(*) FROM Result;");
				int setSize2 = set.getInt(1);
				if(setSize==setSize2)break;
				stop++;
				statement.executeUpdate("DELETE FROM Temp;");
			}
			statement.executeUpdate("DROP TABLE IF EXISTS Temp;");
			statement.executeUpdate("INSERT INTO Connected "+
									"SELECT Airline, Origin, Destination, MIN(Stops) FROM Result "+
									"GROUP BY Airline, Origin, Destination ;");
			statement.executeUpdate("DROP TABLE IF EXISTS Result;");
			statement.close();
			set.close();
		}
		catch (Exception e)
		{
		  throw new RuntimeException("There was a runtime problem!", e);
		}
		finally
		{
		  try
		  {
			if(conn != null)
			{
			  conn.close();
			}
		  }
		  catch (SQLException e)
		  {
			throw new RuntimeException("Cannot close the connection!", e);
		  }
		}
	}
}