package ETL_Assignment;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.crypto.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;

import com.squareup.okhttp.*;

public class test {
	static test ca =new test();
	static ENC_DEC ed=new ENC_DEC();
static ObjectMapper objmap = new ObjectMapper();
public ObjectMapper getobjectmapper() {
ObjectMapper obj =new ObjectMapper();
return obj;
}
public static JsonNode jsonparser(String s) throws JsonMappingException, JsonProcessingException {

return objmap.readTree(s);
}

static Config c=new Config();
static Alation_Data a=new Alation_Data();
static String Titles() throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
	OkHttpClient client = new OkHttpClient();
	client.setConnectTimeout(5, TimeUnit.MINUTES);
	client.setReadTimeout(5, TimeUnit.MINUTES);
	client.setWriteTimeout(5, TimeUnit.MINUTES);
	long skip=0,count=0; //skip is for pagination,count is to count article entry
	String name="";
	String name1="a";// should not be empty
	String enc_token=c.config("TOKEN");
	String Alation_Token=ed.Dec(enc_token);
	String S="";
	while(!name1.isEmpty())
	{
		Request request = new Request.Builder()
				  .url(c.config("alation.api")+skip)
				  .method("GET", null)
				  .addHeader("TOKEN", Alation_Token)
				  .build();
				Response response = client.newCall(request).execute();
				String s=response.body().string();
				S+=s;
				response.body().close();
		JsonNode root=a.jsonparser(s);
		for(int i=0;i<root.size();i++)
		{	
			name1=root.get(i).asText();	
//			name+=name1;
//			System.out.println(name1);
			count++;
			if(count%99==0)
			{
				skip+=99;
				break;
			}			
		}
	}
	return S;
}
static ArrayList<String> Fields(String str) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException
{
	String name=ca.Titles();
	JsonNode root=ca.jsonparser(name);
	ArrayList<String> list=new ArrayList<String>();
	for(int i=0;i<root.size();i++)
	{
		list.add(root.get(i).get(str).asText());
	}
	return list;
}
static String Templet() throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
	System.out.println("Creating Templet to upload Assets and its Attributes please waite...");
	ArrayList<String> al=ca.Fields("title");
	String str="";
	for(int i=0;i<al.size();i++)
	{
		str+="{\r\n"+ "\"resourceType\": \"Asset\",\r\n"+ "\"identifier\": \r\n"+ "{\"name\": \""+al.get(i)+"\",\r\n"+ "\"domain\": {\r\n"+ "\"name\": \"ETL_Domain\",\r\n"+ "\"community\": {\"name\": \"AD_ETL2\"}\r\n"+ "}\r\n"+ "},\r\n"+ "\"type\": {\"name\": \"Business Term\"},\r\n"+ "\"attributes\": {\r\n"+ "\"edc_label\": [\r\n"+ "{\r\n"+ "\"value\": \""+ca.Fields("id").get(i)+"\"\r\n"+ "}\r\n"+ "],\r\n"+ "\"edc_value\": [\r\n"+ "{\r\n"+"\"value\":\""+ca.Fields("url").get(i)+"\""+ "\n}]\n}\n},";	
	System.out.println(str);
	}
	return "["+str+"{\r\n"+ "\"resourceType\": \"Asset\",\r\n"+ "\"identifier\": \r\n"+ "{\"name\": \"a\""+",\r\n"+ "\"domain\": {\r\n"+ "\"name\": \"ETL_Domain\",\r\n"+ "\"community\": {\"name\": \"AD_ETL2\"}\r\n"+ "}\r\n"+ "},\r\n"+ "\"type\": {\"name\": \"Business Term\"}}]";
}
//public void master() throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
//{
//	System.out.println("Fetching Articles from Alation please waite...");
//	a.Titles("title");
//	System.out.println("Fetching Assets from Collibra please waite...");
//	ca.id();
//	System.out.println("Creating Assets in Collibra please waite...");
//	ca.create();
//}
public static void main(String args[]) throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
	File file=new File("E://import_file.json");
	if(file.exists())
	{
		file.delete();
	}
	file.createNewFile();
	String str=ca.Templet();
	FileOutputStream writter=new FileOutputStream("E://import_file.json",true);
	System.out.println("Writting Data to file for uploading purpose. Please waite...");
	writter.write(str.getBytes());
	
	//	System.out.println(ca.Fields("url"));
}
}