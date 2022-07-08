package ETL_Assignment;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.squareup.okhttp.*;

public class IDQ_Score_Cards {
	static ObjectMapper obj=new ObjectMapper();
	static JsonNode parse(String str) throws JsonMappingException, JsonProcessingException
	{
		return obj.readTree(str);
	}
static database d=new database();
static Config c=new Config();
static IDQ_Score_Cards idq=new IDQ_Score_Cards();
static String Fetch_DQRules() throws IOException
{
	String baseApi=c.config("DQRules.API");
	String offset=c.config("offset");
	String limit=c.config("limit");
	String count=c.config("countLimit");
	OkHttpClient client = new OkHttpClient();
			Request request = new Request.Builder()
			  .url(baseApi+"?offset="+offset+"&limit="+limit+"&countLimit="+count+"&nameMatchMode=ANYWHERE&sortField=NAME&sortOrder=ASC")
			  .method("GET", null)
			  .addHeader("accept", "application/json")
			  .addHeader("Authorization", "Basic YWRlc2htdWtoOlRlc3RtZUAxMjM=")
			  .addHeader("Cookie", "AWSALB=1tH5gEUDNiBPRcvjVhUv+9wxXZKglDM7uj6tsEp+eoFxKjpjrXOGWPF17g9rj6HqhD30qLoJCuyckZL+6nb4fD0aU1U9At2bbkHvyk5EzIZODMd706gXxdPoEt+H; AWSALBCORS=1tH5gEUDNiBPRcvjVhUv+9wxXZKglDM7uj6tsEp+eoFxKjpjrXOGWPF17g9rj6HqhD30qLoJCuyckZL+6nb4fD0aU1U9At2bbkHvyk5EzIZODMd706gXxdPoEt+H; JSESSIONID=351da77b-4a3d-44e6-99ab-332b9caa35b1")
			  .build();
			Response response = client.newCall(request).execute();
			String str=response.body().string();
			response.body().close();
			return str;
}
	public void DQ_Rules() throws IOException
	{
		String json=idq.Fetch_DQRules();
		ArrayList<String> al=d.MetaData("SCORECARD_NAME");
		ArrayList<String> al2=new ArrayList<String>();
		JsonNode node=idq.parse(json);
				
		for(int i=0;i<al.size();i++)
		{
			if(!al2.contains(al.get(i)))
			{
				al2.add(al.get(i));
			}
		}		
		ArrayList<String> al3=new ArrayList<String>();
		node=node.get("results");
		for(int i=0;i<node.size();i++)
		{
			al3.add(node.get(i).get("name").asText());
		}
		for(int i=0;i<al2.size();i++)
		{
			OkHttpClient client = new OkHttpClient();
			MediaType mediaType = MediaType.parse("application/json");
			if(!al3.contains(al2.get(i)))
			{
			RequestBody body = RequestBody.create(mediaType, "{\"name\":\""+al2.get(i)+"\",\"categorizationRelationTypeId\":\"8a64fc92-8c4e-4e2c-88f8-755b1f2d5cd2\",\"dataQualityMetrics\":[{\"countOperation\":\"Sum\"}],\"relationTraceEntries\":[{\"roleDirection\":true,\"relationTypeId\":\"d4c8cad5-04df-4439-bf14-9f591c266cd0\"},{\"roleDirection\":true,\"relationTypeId\":\"00000000-0000-0000-0000-000000007016\"}]}");
			Request request = new Request.Builder()
			  .url("https://ia-53.collibra.com/rest/2.0/dataQualityRules")
			  .method("POST", body)
			  .addHeader("accept", "application/json")
			  .addHeader("Content-Type", "application/json")
			  .addHeader("Authorization", "Basic YWRlc2htdWtoOlRlc3RtZUAxMjM=")
			  .addHeader("Cookie", "AWSALB=97xInpurDJEFyfT37OpEyu8MJIaq9NJTsgwqcUhu/R451OYSQt5BPjgTOYWBgQzA2TMRtmwLFx++LAarg+nydOftvI0Et/LxGxmmlA2pXPFFKTgynHXtNIwCopT4; AWSALBCORS=97xInpurDJEFyfT37OpEyu8MJIaq9NJTsgwqcUhu/R451OYSQt5BPjgTOYWBgQzA2TMRtmwLFx++LAarg+nydOftvI0Et/LxGxmmlA2pXPFFKTgynHXtNIwCopT4; JSESSIONID=3f76b47d-5e55-4992-b0e8-b614a31b360a")
			  .build();
			Response response = client.newCall(request).execute();	
			response.body().close();
		}
		}
	}
	public void master() throws IOException
	{
		System.out.println("Fetching DQ Rules");
		idq.Fetch_DQRules();
		System.out.println("Creating new Data Quality Rules please waite...");
		idq.DQ_Rules();
	}
}