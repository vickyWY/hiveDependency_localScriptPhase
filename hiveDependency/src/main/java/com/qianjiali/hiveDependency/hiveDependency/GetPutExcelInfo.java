package com.qianjiali.hiveDependency.hiveDependency;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableWorkbook;

public class GetPutExcelInfo {

  public void CreateExcel(String FileName)
  {
	  try{
		  File file = new File(FileName);
		  if(file.exists()&&file.isFile()){
			  file.delete();
		  }
		  WritableWorkbook wb = Workbook.createWorkbook(file);  
		  jxl.write.WritableSheet ws = wb.createSheet("Sheet_1", 0);	  
		  Label titleC1 = new Label(0, 0, "FileName");
		  Label titleC2 = new Label(1, 0, "SourceTableName");
		  Label titleC3 = new Label(2, 0, "TargetTableName");
		  Label titleC4 = new Label(3, 0, "Operate");
		  ws.addCell(titleC1);
		  ws.addCell(titleC2);
		  ws.addCell(titleC3);	  
		  ws.addCell(titleC4);	
		  wb.write();
		  wb.close();
	  } catch (Exception e) {  
          System.out.println(e);  
      } 	  
  }
  
  public void AddCells(String ResultFile,String FileName,String SourceTable, String TargetTable,String operate)
  {
	  try{
		   FileInputStream fs=new FileInputStream(ResultFile);
		   POIFSFileSystem ps=new POIFSFileSystem(fs);
		   HSSFWorkbook wb=new HSSFWorkbook(ps);
		   HSSFSheet sheet=wb.getSheetAt(0);//获取到工作表，因为一个excel可能有多个工作表  
		   HSSFRow row=sheet.getRow(0);//获取第一行（excel中的行默认从0开始，所以这就是为什么，一个excel必须有字段列头），即，字段列头，便于赋值  
		   //System.out.println(sheet.getLastRowNum()+" "+row.getLastCellNum());//分别得到最后一行的行号，和一条记录的最后一个单元格 

		   FileOutputStream out=new FileOutputStream(ResultFile); //向d://test.xls中写数据  
		   row=sheet.createRow((short)(sheet.getLastRowNum()+1)); //在现有行号后追加数据  
		   row.createCell(0).setCellValue(FileName); //设置第一个（从0开始）单元格的数据  
		   row.createCell(1).setCellValue(SourceTable); //设置第二个（从0开始）单元格的数据 
		   row.createCell(2).setCellValue(TargetTable); //设置第二个（从0开始）单元格的数据  
		   row.createCell(3).setCellValue(operate); //设置第二个（从0开始）单元格的数据  

		   out.flush();
		   wb.write(out);
		   out.close();
		   wb.close();
		 //  System.out.println(row.getPhysicalNumberOfCells()+" "+row.getLastCellNum());
		   
		   
	  } catch (Exception e) {  
          System.out.println(e);  
      } 	  
  }
  
}
