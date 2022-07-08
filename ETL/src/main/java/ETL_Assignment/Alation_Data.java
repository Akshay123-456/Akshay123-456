package ETL_Assignment;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class Alation_Data {
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
	static ArrayList<String> Titles(String str) throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
	{
		OkHttpClient client = new OkHttpClient();
		client.setConnectTimeout(5, TimeUnit.MINUTES);
		client.setReadTimeout(5, TimeUnit.MINUTES);
		client.setWriteTimeout(5, TimeUnit.MINUTES);
		long skip=0,count=0; //skip is for pagination,count is to count article entry
		ArrayList<String> name=new ArrayList<String>();
		String name1="a";// should not be empty
		String enc_token=c.config("TOKEN");
		String Alation_Token=ed.Dec(enc_token);
		while(!name1.isEmpty())
		{
			Request request = new Request.Builder()
					  .url(c.config("alation.api")+skip)
					  .method("GET", null)
					  .addHeader("TOKEN", Alation_Token)
					  .build();
					Response response = client.newCall(request).execute();
					String s=response.body().string();
					response.body().close();
			JsonNode root=a.jsonparser(s);
			for(int i=0;i<root.size();i++)
			{	
				name1=root.get(i).get(str).asText();	
				name.add(name1);
				count++;
				if(count%99==0)
				{
					skip+=99;
					break;
				}			
			}
		}
		return name;
	}
}
