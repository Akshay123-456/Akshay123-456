package ETL_Assignment;

import java.io.IOException;
import java.util.*;
public class Config {
	static String config(String str) throws IOException
	{
		Properties prop=new Properties();
		prop.load(Collibra_Data.class.getClassLoader().getResourceAsStream("ETL.properties"));
		return prop.getProperty(str);
	}
}
