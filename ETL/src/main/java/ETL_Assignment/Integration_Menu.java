package ETL_Assignment;

import java.util.Scanner;

public class Integration_Menu {
static Collibra_Data ca =new Collibra_Data();
static IDQ_Score_Cards score=new IDQ_Score_Cards();
	public static void main(String[] args) {
		
		System.out.println("-----------------------------------------------------------------------------------");
		System.out.println("1. Fetching Articles from Alation & Loading them to Collibra as Asset.\n2. Fetching Score Cards from IDQ & Loading them to Collibra as Data Quality Rules.\n3. Exit");
		System.out.println("-----------------------------------------------------------------------------------");
		int ch=0;
		try
		{
			while(ch!=3)
			{
				System.out.println("Please Enter your choice: 1, 2 or 3");
				Scanner sc=new Scanner(System.in);
				ch=sc.nextInt();
				switch(ch)
				{
				case 1:
				{
					ca.master();
					System.out.println("Succesfully Loaded Assets to Collibra.");
					break;
				}
				case 2:
				{
					score.master();
					System.out.println("Loaded Score Cards from IDQ to Collibra Succesfully.");
					break;
				}
				case 3:
				{
					System.out.println("Integration Stopped.");
					System.exit(0);
				}
				default:
				{
					System.out.println("Wrong Choice. Please check above options.");
				}
				}
			}	
		}
		catch(Exception e)
		{
			System.out.println("Wrong Input. Integration Stopped.");
		}
	}

}
