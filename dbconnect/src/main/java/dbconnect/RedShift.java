package dbconnect;

import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.json.*;

import com.squareup.okhttp.*;

public class RedShift {
static RedShift rshift=new RedShift();
public void select()
{
	try
	{
		Class.forName("com.amazon.redshift.jdbc42.Driver");
		Connection conn=DriverManager.getConnection("jdbc:redshift://redshift-ia.cxgkh5iwr3tw.us-east-1.redshift.amazonaws.com:5439/dev","administrator","InfoA$$eT2021#");
		System.out.println("Connected Succesfully");
		Statement stmt=conn.createStatement();
		ResultSet rs=stmt.executeQuery("SELECT * FROM public.sales limit 100;");
		System.out.println("Sales ID\tList ID\t\tSeller ID");
		System.out.println("--------------------------------------------------");
		while(rs.next())
		{
			System.out.println(rs.getInt("salesid")+"\t\t"+rs.getInt("listid")+"\t\t"+rs.getInt("sellerid"));
		}
		conn.close();
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
}

static String Community() throws IOException
{
	OkHttpClient client = new OkHttpClient();
	client.setReadTimeout(5, TimeUnit.MINUTES);
			Request request = new Request.Builder()
			  .url("https://ia-53.collibra.com/rest/2.0/communities?offset=0&limit=5000&countLimit=-1&nameMatchMode=ANYWHERE&excludeMeta=true&sortField=NAME&sortOrder=ASC")
			  .method("GET", null)
			  .addHeader("accept", "application/json")
			  .addHeader("Authorization", "Basic YWRlc2htdWtoOlRlc3RtZUAxMjM=")
			  .addHeader("Cookie", "AWSALB=InMhUbMkWbh6qAEkosel6J3Ar1EqzQavJI7LCtPiLi48l2SyhWkenhBVDmym1df/Scmxdrgd28sA8qa1eERQ12Aix20+qJ5dwAREMQ24vuCSgIs0dJgv+OPDHags; AWSALBCORS=InMhUbMkWbh6qAEkosel6J3Ar1EqzQavJI7LCtPiLi48l2SyhWkenhBVDmym1df/Scmxdrgd28sA8qa1eERQ12Aix20+qJ5dwAREMQ24vuCSgIs0dJgv+OPDHags; AWSALBTG=Ih89ETteh+0h1aHmIXjCjadtMW9KUiW8xuNXMiyFvGCl5JcTvKGvsrn0mcvXnxPQing+pFt0Fqxgz00SHDDGNighAduIf9AQRkEPv3mV8GZt3wIy/6+eyk3bI+btcUopD23qLJCDyA2BOBDJyDAnaj18VN4piXXSZ60dIP1QcCMpw+UZTtE=; AWSALBTGCORS=Ih89ETteh+0h1aHmIXjCjadtMW9KUiW8xuNXMiyFvGCl5JcTvKGvsrn0mcvXnxPQing+pFt0Fqxgz00SHDDGNighAduIf9AQRkEPv3mV8GZt3wIy/6+eyk3bI+btcUopD23qLJCDyA2BOBDJyDAnaj18VN4piXXSZ60dIP1QcCMpw+UZTtE=; JSESSIONID=d06cc770-c01f-401d-af36-1f42f21ff078")
			  .build();
			Response response = client.newCall(request).execute();
			String resp=response.body().string();
			return resp;
}

static String Domain() throws IOException
{
	OkHttpClient client = new OkHttpClient();
	client.setReadTimeout(5, TimeUnit.MINUTES);
			Request request = new Request.Builder()
			  .url("https://ia-53.collibra.com/rest/2.0/domains?offset=1207&limit=5000&countLimit=-1&nameMatchMode=ANYWHERE&excludeMeta=true&includeSubCommunities=false")
			  .method("GET", null)
			  .addHeader("accept", "application/json")
			  .addHeader("Authorization", "Basic YWRlc2htdWtoOlRlc3RtZUAxMjM=")
			  .addHeader("Cookie", "AWSALB=oHJ5yKbBoDhJCg5Ab8zvEmT21yARL5sIBk20571SU0uHYoTXn2SymvpNU6izVsCYnEbyBghZQCwkURUMBvXfMvYIANaJZfDd6aaJUKFqLDtXJ25miJNpuaC2ysnd; AWSALBCORS=oHJ5yKbBoDhJCg5Ab8zvEmT21yARL5sIBk20571SU0uHYoTXn2SymvpNU6izVsCYnEbyBghZQCwkURUMBvXfMvYIANaJZfDd6aaJUKFqLDtXJ25miJNpuaC2ysnd; AWSALBTG=lso0ur6mBw/mKis2FDTB77QvaNSbWA7iUUwquEyCXMRCuscxwiUFqMtzDcEx98pOv3A3iR13Wr67Op/iBySBHkuu9x1NQ5iZZJr4D+NJuhcyAx1GQ4ihibHq8nQ5s+8qMG4VnA5uycwzF/2bKpvwaC13Y5DNXUNT9vzjhHMHlIERgwpYGIc=; AWSALBTGCORS=lso0ur6mBw/mKis2FDTB77QvaNSbWA7iUUwquEyCXMRCuscxwiUFqMtzDcEx98pOv3A3iR13Wr67Op/iBySBHkuu9x1NQ5iZZJr4D+NJuhcyAx1GQ4ihibHq8nQ5s+8qMG4VnA5uycwzF/2bKpvwaC13Y5DNXUNT9vzjhHMHlIERgwpYGIc=; JSESSIONID=d06cc770-c01f-401d-af36-1f42f21ff078")
			  .build();
			Response response = client.newCall(request).execute();
			String resp=response.body().string();
			return resp;
}

static String Collibra() throws JSONException, IOException
{
	OkHttpClient client = new OkHttpClient();
	client.setReadTimeout(5, TimeUnit.MINUTES);
	client.setReadTimeout(5, TimeUnit.MINUTES);
			Request request = new Request.Builder()
			  .url("https://ia-53.collibra.com/rest/2.0/assets?offset=0&limit=5000&countLimit=-1&nameMatchMode=ANYWHERE&typeInheritance=false&excludeMeta=true&sortField=NAME&sortOrder=ASC")
			  .method("GET", null)
			  .addHeader("accept", "application/json")
			  .addHeader("Authorization", "Basic YWRlc2htdWtoOlRlc3RtZUAxMjM=")
			  .addHeader("Cookie", "AWSALB=Tpw2lbESbqNZBHqQh8akFN2N0deytYZhe6z92UGVC5S64bdXFsKVLiQ1HsQQvm0xlj7TQMKxbkSPJ+LiqluyzTwrl4rtg1G2cG7/qISZwOsS3Evqa5uw37yGLCX+; AWSALBCORS=Tpw2lbESbqNZBHqQh8akFN2N0deytYZhe6z92UGVC5S64bdXFsKVLiQ1HsQQvm0xlj7TQMKxbkSPJ+LiqluyzTwrl4rtg1G2cG7/qISZwOsS3Evqa5uw37yGLCX+; AWSALBTG=+kfUyHkK9rxeUMDyn6TLXt1UWUCq3QuxCEtMay4NBc0k508Wo4YuWctHYcvv1XBNqlItnTmCmPNFWyw/DFgibd1f+PR9o1D9Gs7O3RgQMy4XWFdgAkvGQtO/jCGHnOpvyji5xZlW6c9DR35Q9gqav0yt1ZJfDKSsjmaGJCKN3+p9eNikoRs=; AWSALBTGCORS=+kfUyHkK9rxeUMDyn6TLXt1UWUCq3QuxCEtMay4NBc0k508Wo4YuWctHYcvv1XBNqlItnTmCmPNFWyw/DFgibd1f+PR9o1D9Gs7O3RgQMy4XWFdgAkvGQtO/jCGHnOpvyji5xZlW6c9DR35Q9gqav0yt1ZJfDKSsjmaGJCKN3+p9eNikoRs=; JSESSIONID=c632760d-2897-4f6e-8cc9-3b1ae417a00b")
			  .build();
			Response response = client.newCall(request).execute();
			String resp=response.body().string();
			return resp;
}

public void insert() throws JSONException, IOException
{
	int count=0;
	try
	{
//		JSONArray arr=rshift.Collibra();
		Class.forName("com.amazon.redshift.jdbc42.Driver");
		Connection conn=DriverManager.getConnection("jdbc:redshift://redshift-ia.cxgkh5iwr3tw.us-east-1.redshift.amazonaws.com:5439/dev","administrator","InfoA$$eT2021#");
		System.out.println("Connected Succesfully");
		
		//Connection data code
		
		String resp=rshift.Community();
				JSONObject obj=new JSONObject(resp);
				JSONArray ja=obj.getJSONArray("results");
				//JSONArray array=new JSONArray();
				PreparedStatement pstmt=conn.prepareStatement("INSERT INTO public.akshay_data VALUES(?,?,?,?,?)");
//				for(int i=0;i<ja.length();i++)
//				{
//					System.out.println((i+1)+". Community "+ja.getJSONObject(i).getString("name"));
//					
//					pstmt.setString(1, "null");
//					pstmt.setString(2, ja.getJSONObject(i).getString("resourceType"));
//					pstmt.setString(3, ja.getJSONObject(i).getString("id"));
//					pstmt.setString(4, "null");
//					pstmt.setString(5, ja.getJSONObject(i).getString("name"));
//					count+=pstmt.executeUpdate();
//				}
				resp=rshift.Domain();
				JSONObject obj2=new JSONObject(resp);
				JSONArray ja2=obj2.getJSONArray("results");
				for(int i=1207;i<ja2.length();i++)
				{
					System.out.println((i+1)+". Domain "+ja2.getJSONObject(i).getString("name"));
					
					pstmt.setString(1, "null");
					pstmt.setString(2, ja2.getJSONObject(i).getString("resourceType"));
					pstmt.setString(3, ja2.getJSONObject(i).getString("id"));
					pstmt.setString(4, ja2.getJSONObject(i).getJSONObject("type").getString("name"));
					pstmt.setString(5, ja2.getJSONObject(i).getString("name"));
					count+=pstmt.executeUpdate();
				}
				resp=rshift.Collibra();
				JSONObject obj3=new JSONObject(resp);
				JSONArray ja3=obj3.getJSONArray("results");
				for(int i=0;i<ja3.length();i++)
				{
					System.out.println((i+1)+". Assets "+ja3.getJSONObject(i).getString("name"));
					
					pstmt.setString(1, ja3.getJSONObject(i).getString("displayName"));
					pstmt.setString(2, ja3.getJSONObject(i).getString("resourceType"));
					pstmt.setString(3, ja3.getJSONObject(i).getString("id"));
					pstmt.setString(4, ja3.getJSONObject(i).getJSONObject("type").getString("name"));
					pstmt.setString(5, "null");
					count+=pstmt.executeUpdate();
				}
//		Statement stmt=conn.createStatement();	
		//Connectivity Code
		
//		for(int i=0;i<arr.length();i++)
//		{
//			pstmt.setString(1, arr.getJSONObject(i).getString("Display Name"));
//			pstmt.setString(2, arr.getJSONObject(i).getString("Resource Type"));
//			pstmt.setString(3, arr.getJSONObject(i).getString("ID"));
//			pstmt.setString(4, arr.getJSONObject(i).getString("type"));
//			pstmt.setString(5, arr.getJSONObject(i).getString("Name"));
//			count=pstmt.executeUpdate();
//		}
		System.out.println("Inserted "+count/3+" Records Succesfully.");
		conn.close();
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
}

	public static void main(String[] args) throws JSONException, IOException {
		// TODO Auto-generated method stub
		rshift.insert();
	}

}
