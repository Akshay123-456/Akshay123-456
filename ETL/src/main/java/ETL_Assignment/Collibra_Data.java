package ETL_Assignment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.squareup.okhttp.*;

public class Collibra_Data {
	static Collibra_Data ca =new Collibra_Data();
	static Config c=new Config();
	static Alation_Data a=new Alation_Data();
static ObjectMapper objmap = new ObjectMapper();
public ObjectMapper getobjectmapper() {
ObjectMapper obj =new ObjectMapper();
return obj;
}
public static JsonNode jsonparser(String s) throws JsonMappingException, JsonProcessingException {

return objmap.readTree(s);
}

static ArrayList<String> id() throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
Collibra_Data ca =new Collibra_Data();
	
	OkHttpClient client = new OkHttpClient();
	client.setConnectTimeout(5, TimeUnit.MINUTES);
	client.setReadTimeout(5, TimeUnit.MINUTES);
			Request request = new Request.Builder()
			  .url("https://ia-53.collibra.com/rest/2.0/assets?offset="+c.config("offset")+"&limit="+c.config("limit")+"&countLimit="+c.config("countLimit")+"&nameMatchMode=ANYWHERE&domainId="+c.config("domainId")+"&typeInheritance=true&excludeMeta=true&sortField=NAME&sortOrder=ASC")
			  .method("GET", null)
			  .addHeader("accept", "application/json")
			  .addHeader("Authorization", c.config("Authorization"))
			  .addHeader("Cookie", "AWSALB=VRkl5j7mQMh86TKs7nvtmRWueSMnN5ftsw/XVEoT4xnZJJefuMRv2UOwepn7Wj/+TTNHHHORenGY5psxpFGdHi65B72m9jriowmOFAnBf8UzCtYeqDFgomRrgvbT; AWSALBCORS=VRkl5j7mQMh86TKs7nvtmRWueSMnN5ftsw/XVEoT4xnZJJefuMRv2UOwepn7Wj/+TTNHHHORenGY5psxpFGdHi65B72m9jriowmOFAnBf8UzCtYeqDFgomRrgvbT; JSESSIONID=547ffcb8-03f2-4bd8-8928-37cd807a0215")
			  .build();
			Response response = client.newCall(request).execute();
			String resp=response.body().string();
			response.body().close();
			JsonNode node=ca.jsonparser(resp).get("results");
			ArrayList<String> al=new ArrayList<String>();
			for(int i=0;i<node.size();i++)
			{				
			al.add(node.get(i).get("displayName").asText());
			}
			return al;
}
public void create() throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
	ArrayList<String> al=a.Titles("title");
	ArrayList<String> resp=ca.id();
	 String domain=c.config("domainId");
	 String type_id=c.config("typeId");
	 String collibra_url=c.config("collibra_url");
	 String hyperlink=c.config("excludedFromAutoHyperlinking");
//	 JsonNode node=ca.jsonparser(resp);
	 ArrayList<String> name=new ArrayList<String>();
	 System.out.println("Uploading Assets to Collibra Please Wait...");
	for(int i=0;i< resp.size();i++) {
	OkHttpClient client = new OkHttpClient();
	client.setConnectTimeout(5, TimeUnit.MINUTES);
	client.setReadTimeout(5, TimeUnit.MINUTES);
	client.setWriteTimeout(5, TimeUnit.MINUTES);
	MediaType mediaType = MediaType.parse("application/json");	
//	String Name=al.get(i);
	if(!resp.contains(al.get(i)))
	{
	RequestBody body = RequestBody.create(mediaType, "{\"name\":\""+al.get(i).toUpperCase()+"\",\"displayName\":\""+al.get(i)+"\",\"domainId\":\""+domain+"\",\"typeId\":\""+type_id+"\",\"excludedFromAutoHyperlinking\":"+hyperlink+"}");
	Request request = new Request.Builder()
	.url(c.config("API.Create.Asset"))
	.method("POST", body)
	.addHeader("accept", "application/json")
	.addHeader("Content-Type", "application/json")
	.addHeader("Authorization", c.config("Authorization"))
	.addHeader("Cookie", "AWSALB=FyTDu6VydFU7ByZQrG581unEI/cVcCnFNH2SZJaaTZRNWR78WIKcFpATPh+UTPOf/xNjimpSN0jD4NPsQoMEyTywsmtmOnL3RVXDC0vjzD+jpAJVwOAoYH6j9ABZ; AWSALBCORS=FyTDu6VydFU7ByZQrG581unEI/cVcCnFNH2SZJaaTZRNWR78WIKcFpATPh+UTPOf/xNjimpSN0jD4NPsQoMEyTywsmtmOnL3RVXDC0vjzD+jpAJVwOAoYH6j9ABZ; JSESSIONID=72767933-b2af-46dd-8734-75df73c81d10")
	.build();
	Response response = client.newCall(request).execute();
	System.out.println(al.get(i));
	response.body().close();
	}
	}
}
public void master() throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
	System.out.println("Fetching Articles from Alation please waite...");
	a.Titles("title");
	System.out.println("Fetching Assets from Collibra please waite...");
	ca.id();
	System.out.println("Creating Assets in Collibra please waite...");
	ca.create();
}
}