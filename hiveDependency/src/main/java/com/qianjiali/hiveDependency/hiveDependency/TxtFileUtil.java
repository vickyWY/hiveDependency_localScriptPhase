package com.qianjiali.hiveDependency.hiveDependency;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import  java .io.FileWriter; 


public class TxtFileUtil {
	  public static BufferedReader bufRead;
	  private static String readStr ="";
	  
	  
	public void CreateTxtFile(String filePath)
	  {
		  File file = new File(filePath); 	
		  try{				  		  
			  if(file.exists()&&file.isFile()){
				  file.delete();
			  }
			  file.createNewFile();
		  } catch (Exception e) {  
	          System.out.println(e);  
	      }
	  }
	
	public void WriteTxtFile(String newStrs,String filePath)
	{
		//String read = "";		
		try{
			/*FileReader fileRead = new FileReader(file);
			bufRead = new BufferedReader(fileRead);
			while(bufRead.readLine() != null)
			{
				read = read + bufRead.readLine();				
			}*/
			FileWriter fileWriter = new FileWriter(filePath,true);
			fileWriter.write(newStrs);
			fileWriter.close();
			
		} catch (Exception e){
		 System.out.println(e);  		
		}
		
		
	}
}
