package dbconnect;

import java.io.*;
import java.sql.*;

public class Postgress {

	static Postgress pjdbc=new Postgress();
	public void Generate_CSV() throws IOException
	{
		try
		{
			Class.forName("org.postgresql.Driver");
			Connection conn=DriverManager.getConnection("jdbc:postgresql://localhost:5432/COVID_Data","postgres","Akshay@123");
			Statement stmt=conn.createStatement();
			ResultSet rs=stmt.executeQuery("SELECT * FROM public.\"Covid Data by Region\" limit 100");
			System.out.println("Date Reported\tCountry Code\tCountry");
			System.out.println("------------------------------------------------------");
			while(rs.next())
			{
				System.out.println(rs.getString("Date_reported")+"\t"+rs.getString("Country_code")+"\t\t"+rs.getString("Country"));
//				obj.put("Date_reported", rs.getString("Date_reported"));
//				obj.put("Country_code", rs.getString("Country_code"));
//				obj.put("Country", rs.getString("Country"));
//				obj.put("WHO_region", rs.getString("WHO_region"));
//				obj.put("New_cases", rs.getInt("New_cases"));
//				obj.put("Cumulative_cases", rs.getInt("Cumulative_cases"));
//				obj.put("New_deaths", rs.getInt("New_deaths"));
//				obj.put("Cumulative_deaths", rs.getInt("Cumulative_deaths"));
//				ja.put(obj);
			}
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
	         System.err.println(e.getClass().getName()+": "+e.getMessage());
	         System.exit(0);
		}
//		String CSV=CDL.toString(ja);
//		BufferedWriter writter=new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D://Postgress Data.csv")));
//		writter.write(CSV);
//		writter.close();	
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		pjdbc.Generate_CSV();
	}

}
