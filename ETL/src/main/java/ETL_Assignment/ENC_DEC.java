package ETL_Assignment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.*;

import java.util.*;

public class ENC_DEC {
	static Config c=new Config();
	static ENC_DEC ed=new ENC_DEC();
	static String key="";
	static String initVector="encryptionIntVec";
static String Enc() throws IOException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException
{
	Scanner sc=new Scanner(System.in);
	System.out.println("Enter String to be Encrypted:");
	String token=sc.next();
	String Path=c.config("Encryption.Key.Path");
	key=new String(Files.readAllBytes(Paths.get(Path)));
	IvParameterSpec iv=new IvParameterSpec(initVector.getBytes("UTF-8"));
	SecretKeySpec spec=new SecretKeySpec(key.getBytes("UTF-8"),"AES");
	Cipher cipher=Cipher.getInstance("AES/CBC/PKCS5PADDING");
	cipher.init(Cipher.ENCRYPT_MODE, spec, iv);	
	byte[] encrypted=cipher.doFinal(token.getBytes());
	return Base64.getEncoder().encodeToString(encrypted);
}
static String Dec(String enc) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException
{
	String Path=c.config("Encryption.Key.Path");
	key=new String(Files.readAllBytes(Paths.get(Path)));
	IvParameterSpec iv=new IvParameterSpec(initVector.getBytes("UTF-8"));
	SecretKeySpec spec=new SecretKeySpec(key.getBytes("UTF-8"),"AES");
	Cipher cipher=Cipher.getInstance("AES/CBC/PKCS5PADDING");
	cipher.init(Cipher.DECRYPT_MODE, spec, iv);
	byte[] orignal=cipher.doFinal(Base64.getDecoder().decode(enc.getBytes()));
	return new String(orignal);
}
	public static void main(String[] args) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException {
		// TODO Auto-generated method stub
		System.out.println("Key Fetched from File");
		System.out.println(ed.Enc());
	}

}
