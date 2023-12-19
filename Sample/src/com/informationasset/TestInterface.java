package com.informationasset;
/*THis Interface is created for testing purpose. You can add any method in it.*/
public interface TestInterface {
void show();
String message();
private String getMessage(String msg)
{
int statusCode=getCode(10);
return msg+"\n"+statusCode;	
}
  private int getCode(int statusCode)
  {
	  getMessage("Error while execution");
    return statusCode;
  }
}
