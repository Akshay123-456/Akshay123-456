package dbconnect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Snowflake {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
try
{
	Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
	Properties properties = new Properties();
    properties.put("user", "Akshay");     // replace "" with your username
    properties.put("password", "Akshay@123"); // replace "" with your password
    properties.put("account", "lj00646.us-east-2.aws");  // replace "" with your account name
    properties.put("db", "JnJ");       // replace "" with target database name
    properties.put("schema", "PUBLIC");   // replace "" with target schema name
	Connection conn=DriverManager.getConnection("jdbc:snowflake://lj00646.us-east-2.aws.snowflakecomputing.com",properties);
	Statement stmt=conn.createStatement();
	ResultSet rs=stmt.executeQuery("SELECT * FROM \"JNJ\".\"PUBLIC\".\"ORDERS\"");
	System.out.println("|---------------------------------------------|");
	System.out.println("|ORDER_ID\t|CREDIT_CARD_AMOUNT\t|COUPON_AMOUNT|");
	System.out.println("|---------------------------------------------|");
	while(rs.next())
	{
		System.out.println("|"+rs.getInt(1)+"\t\t|"+rs.getInt(2)+"\t\t\t|"+rs.getInt(3));
		System.out.println("|----------------------------------------------|");
	}
}
catch(Exception e)
{
	e.printStackTrace();
}
	}

}
