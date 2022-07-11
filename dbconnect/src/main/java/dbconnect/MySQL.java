package dbconnect;

import java.io.*;
import java.sql.*;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.json.*;

import com.squareup.okhttp.*;

public class MySQL {
	static MySQL msql=new MySQL();
	static String Community() throws IOException
	{
		OkHttpClient client = new OkHttpClient();
		client.setReadTimeout(5, TimeUnit.MINUTES);
				Request request = new Request.Builder()
				  .url("https://ia-53.collibra.com/rest/2.0/communities?offset=0&limit=15&countLimit=-1&nameMatchMode=ANYWHERE&excludeMeta=true&sortField=NAME&sortOrder=ASC")
				  .method("GET", null)
				  .addHeader("accept", "application/json")
				  .addHeader("Authorization", "Basic YWRlc2htdWtoOlRlc3RtZUAxMjM=")
				  .addHeader("Cookie", "AWSALB=InMhUbMkWbh6qAEkosel6J3Ar1EqzQavJI7LCtPiLi48l2SyhWkenhBVDmym1df/Scmxdrgd28sA8qa1eERQ12Aix20+qJ5dwAREMQ24vuCSgIs0dJgv+OPDHags; AWSALBCORS=InMhUbMkWbh6qAEkosel6J3Ar1EqzQavJI7LCtPiLi48l2SyhWkenhBVDmym1df/Scmxdrgd28sA8qa1eERQ12Aix20+qJ5dwAREMQ24vuCSgIs0dJgv+OPDHags; AWSALBTG=Ih89ETteh+0h1aHmIXjCjadtMW9KUiW8xuNXMiyFvGCl5JcTvKGvsrn0mcvXnxPQing+pFt0Fqxgz00SHDDGNighAduIf9AQRkEPv3mV8GZt3wIy/6+eyk3bI+btcUopD23qLJCDyA2BOBDJyDAnaj18VN4piXXSZ60dIP1QcCMpw+UZTtE=; AWSALBTGCORS=Ih89ETteh+0h1aHmIXjCjadtMW9KUiW8xuNXMiyFvGCl5JcTvKGvsrn0mcvXnxPQing+pFt0Fqxgz00SHDDGNighAduIf9AQRkEPv3mV8GZt3wIy/6+eyk3bI+btcUopD23qLJCDyA2BOBDJyDAnaj18VN4piXXSZ60dIP1QcCMpw+UZTtE=; JSESSIONID=d06cc770-c01f-401d-af36-1f42f21ff078")
				  .build();
				Response response = client.newCall(request).execute();
				String resp=response.body().string();
				return resp;
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		int count=0;
		
		try
		{
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/axon", "root", "");
			Statement stmt=conn.createStatement();
			ResultSet rs=stmt.executeQuery("SELECT * from collibra");
			while(rs.next())
			{
				String resp=msql.Community();
				JSONObject jobj=new JSONObject(resp);
				JSONArray ja=jobj.getJSONArray("results");
				PreparedStatement pstmt=conn.prepareStatement("INSERT INTO collibra VALUES(?,?,?,?,?)");
				for(int i=0;i<ja.length();i++)
				{
					System.out.println(rs.getString(3)+"=="+ja.getJSONObject(i).getString("id"));
//					if(!rs.getString("id").equals(ja.getJSONObject(i).getString("id"))||rs.getString("id").isEmpty())
//					{
						pstmt.setString(1, "null");
						pstmt.setString(2, ja.getJSONObject(i).getString("resourceType"));
						pstmt.setString(3, ja.getJSONObject(i).getString("id"));
						pstmt.setString(4, "null");
						pstmt.setString(5, ja.getJSONObject(i).getString("name"));
						count+=pstmt.executeUpdate();	
//					}
				}
			}
			System.out.println(count+" Records Inserted");
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
