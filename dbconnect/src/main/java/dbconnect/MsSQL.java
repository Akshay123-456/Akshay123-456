package dbconnect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MsSQL {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			Connection conn=null;
			String url="jdbc:sqlserver://216.55.167.90/ABCInsurance";
			String user="adeshmukh";
			String pass="Akshay@1234";
			conn=DriverManager.getConnection(url,user,pass);
			Statement stmt=conn.createStatement();
				String query="Select * from ABCInsurance.dbo.Customer;";
				ResultSet rs=stmt.executeQuery(query);
//				System.out.println("SCORECARD_NAME"+"\t"+"|"+"TOTAL_ROWS"+"\t"+"|"+"INVALID_ROWS"+"\t"+"|"+"VALID_PERCENTAGE");
				while(rs.next())
				{
				System.out.println(rs.getString(1));
				}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
