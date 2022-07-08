package ETL_Assignment;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
public class database {
static ArrayList<String> MetaData(String str) throws IOException
{
	Properties prop=new Properties();
	prop.load(Integration_Menu.class.getClassLoader().getResourceAsStream("ETL.properties"));
	ArrayList<String> data=new ArrayList<String>();
	try
	{
		String Database_Name=prop.getProperty("Database.Name");
		String Table_Name=prop.getProperty("Table.Name");
		String host=prop.getProperty("host");
		Connection conn=null;
		String url="jdbc:sqlserver://"+host+";databaseName="+Database_Name;
		String user=prop.getProperty("User.Name");
		String pass=prop.getProperty("User.Password");
		conn=DriverManager.getConnection(url,user,pass);
		Statement stmt=conn.createStatement();
		if(conn!=null)
		{
			String query="Select * from "+Database_Name+"."+Table_Name+";";
			ResultSet rs=stmt.executeQuery(query);
//			System.out.println("SCORECARD_NAME"+"\t"+"|"+"TOTAL_ROWS"+"\t"+"|"+"INVALID_ROWS"+"\t"+"|"+"VALID_PERCENTAGE");
			while(rs.next())
			{
			data.add(rs.getString(str));
			}
		}
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
	return data;
}
}