package com.qianjiali.hiveDependency.hiveDependency;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * 目的：获取AST中的表，列，以及对其所做的操作，如SELECT,INSERT
 * 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到表级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作，
 *                     遇到TOK_TAB或TOK_TABREF则判断出当前操作的表，遇到子句则压栈当前处理，处理子句。
 *                    子句处理完，栈弹出。 
 *
 */
public class HiveParse {
	   

		private static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		private static String txtFileName = "SQLSource"+df.format(new Date()).toString()+".txt";
		
	    private static String localBase = "E:/mine/qianjiali";  //本地SQL文件地址
	   // private static String resultExcelFile = "D:/java/sql/SQLSource.xls"; //结果集存放(xls格式)地址
	    private static String resultTxtFile = "D:/java/sql/"+ txtFileName;//结果集存放(txt格式)地址
	
	    private Oper oper;
	    private JoinType joinType;
	    private boolean joinClause = false;
	    
	    private Set<String> tableTargetObjects = new HashSet<String>();
	    private Set<String> tableSourceObjects = new HashSet<String>();
	    private Stack<String> tableNameStack = new Stack<String>();
	    
	    private static GetPutExcelInfo finalResultExcel= new GetPutExcelInfo();
	    private static TxtFileUtil TxtFile = new TxtFileUtil();
	    private static ArrayList<File> SQLfiles= SQLFilesOperate.getListFiles(localBase);	
        private static int JoinSequence = 0;
        
	    private enum Oper {
	        SELECT, INSERT, DROP, TRUNCATE, LOAD, CREATETABLE, ALTER, DELETE,RIGHTOUTJOIN,LEFTOUTJOIN,INNERJOIN
	    }
	    private enum JoinType {
	        RIGHTOUTJOIN,LEFTOUTJOIN,INNERJOIN
	    }
	    
	    private  void parseChildNodes(ASTNode ast)
	    {
	        int numCh = ast.getChildCount();    	        
	        if (numCh > 0) {
	            for (int num = 0; num < numCh; num++) {
	                ASTNode child = (ASTNode) ast.getChild(num);       
                    if (ast.getToken() != null) {    	                	
	                	operateParse(ast);
	                	tableParse(ast);
	                }
                    
	                parseChildNodes(child);  	
	                endOperateParse(ast);
	            }
	        }
	    }
	    
	    public void operateParse(ASTNode ast)
	    {
	    	switch (ast.getToken().getType()) {   
             case HiveParser.TOK_RIGHTOUTERJOIN:
            	 joinClause = true;  
            	 joinType = JoinType.RIGHTOUTJOIN;
            	 break;
             case HiveParser.TOK_LEFTOUTERJOIN:
            	 joinClause = true;  
            	 joinType = JoinType.LEFTOUTJOIN;
             	 break;
             case HiveParser.TOK_JOIN:
                 joinClause = true; 
                 joinType = JoinType.INNERJOIN;
                 break;
             case HiveParser.TOK_QUERY:
                 oper = Oper.SELECT;         
                 break;
             case HiveParser.TOK_INSERT:
                 oper = Oper.INSERT;
                 break;
             case HiveParser.TOK_SELECT:
                 oper = Oper.SELECT;
                 break;
             case HiveParser.TOK_DROPTABLE:
                 oper = Oper.DROP;
                 break;
             case HiveParser.TOK_TRUNCATETABLE:
                 oper = Oper.TRUNCATE;
                 break;
             case HiveParser.TOK_LOAD:
                 oper = Oper.LOAD;
                 break;
             case HiveParser.TOK_CREATETABLE:
                 oper = Oper.CREATETABLE;
                 break;
             case HiveParser.TOK_DELETE_FROM:
             	 oper = Oper.DELETE;
                 break;
             }	    	
	    }
	    
	    public void tableParse(ASTNode ast)
	    {
        	
            switch (ast.getToken().getType()) {                    
           
            case HiveParser.TOK_TABLE_PARTITION:
                if (ast.getChildCount() != 2) {
                    String table = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    tableTargetObjects.add(table + "\t" + oper);
                }
                break;
            case HiveParser.TOK_TAB:// outputTable
                String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));              
                tableTargetObjects.add(tableTab + "\t" + oper);
                break;
            case HiveParser.TOK_TABREF:// inputTable
                ASTNode tabTree = (ASTNode) ast.getChild(0);
                String tableName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                        : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                if(joinClause && JoinSequence != 0)
                {                	
                	//tableNameStack.push(tableName + "\t" + oper+ "&&" + joinType);       
                	tableNameStack.push(tableName + "\t" + oper);       
                }
                else
                {
                	tableSourceObjects.add(tableName + "\t" + oper);                	
                }
                JoinSequence++;
                break;
            
            case HiveParser.TOK_TABNAME:// outputTable
            	String tableNameDel = ast.getChild(0).getText().toLowerCase();
                if (oper == Oper.DELETE || oper == Oper.CREATETABLE || oper == Oper.DROP) {	             	
	             	tableTargetObjects.add(tableNameDel + "\t" + oper);
                }	                    	
                break;                                                             
            }	
	    }
	    
	    public void endOperateParse(ASTNode ast)
	    {
	    	if (ast.getToken() != null) {
		   	 switch (ast.getToken().getType()) {   
	             case HiveParser.TOK_RIGHTOUTERJOIN:            	 
	             case HiveParser.TOK_LEFTOUTERJOIN:
	             case HiveParser.TOK_JOIN:
	                 joinClause = false; 
	                 for(int i = 0; i <tableNameStack.size();i++)
	                 {
	                	 tableSourceObjects.add(tableNameStack.pop());
	                 }
	                 break;
	               }
	         }	   
	    }
	    
	    
	    public static String unescapeIdentifier(String val) 
	    {
	        if (val == null) {
	            return null;
	        }
	        if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
	            val = val.substring(1, val.length() - 1);
	        }
	        return val;
	    }

	    public void parse(String FileName,ASTNode ast) 
	    {
	    	parseChildNodes(ast);
	        System.out.println("***************表***************");
	        if(tableTargetObjects.size() != 0 && tableSourceObjects.size() !=0)
	        {
		        for (String sourceTable : tableSourceObjects) {
		        	for(String targetTable: tableTargetObjects){
			        	System.out.println(sourceTable + " " + targetTable);
			            String[] sR = sourceTable.split("\t");
			            String[] tR = targetTable.split("\t");
			            //finalResultExcel.AddCells(resultExcelFile,FileName,sR[0],tR[0],tR[1]);	
			            TxtFile.WriteTxtFile(FileName + "|" + sR[0]+"|"+tR[0]+"|"+tR[1]+ "|TBD" +"|"+ df.format(new Date()).toString() +"\r\n", resultTxtFile);			            
		        	}
		        }
	        }
	        else if(tableTargetObjects.size() == 0 && tableSourceObjects.size() !=0)
	        {	        	
	        	for (String sourceTable : tableSourceObjects) {		        	
			        	System.out.println(sourceTable + " ");
			            String[] sR = sourceTable.split("\t");
			           // finalResultExcel.AddCells(resultExcelFile,FileName,sR[0],"0","0");		           
			            TxtFile.WriteTxtFile(FileName + "|" + sR[0]+"|"+ "?" +"|"+ "?" + "|TBD" + "|" + df.format(new Date()).toString() +"\r\n", resultTxtFile);	
		        }
	        }
	        else if(tableTargetObjects.size() != 0 && tableSourceObjects.size() == 0)
	        {	        	
	        	for (String targetTable : tableTargetObjects) {		        	
			        	System.out.println(targetTable + " ");
			            String[] tR = targetTable.split("\t");
			            //finalResultExcel.AddCells(resultExcelFile,FileName,"0",tR[0],tR[1]);		           
			            TxtFile.WriteTxtFile(FileName + "|" + "?" +"|"+tR[0]+"|"+tR[1]+ "|TBD" + "|" +  df.format(new Date()).toString() +"\r\n", resultTxtFile);	
		        }
	        }           
	    }
	    
	    /** read the hdfs file content
	     * notice that the dst is the full path name
	     */
	    public static byte[] readHDFSFile(String dst) throws Exception
	    {
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        
	        // check if the file exists
	        Path path = new Path(dst);
	        if ( fs.exists(path) )
	        {
	            FSDataInputStream is = fs.open(path);
	            // get the file info to create the buffer
	            FileStatus stat = fs.getFileStatus(path);
	            
	            // create the buffer
	            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
	            is.readFully(0, buffer);
	            
	            is.close();
	            fs.close();
	            
	            return buffer;
	        }
	        else
	        {
	            throw new Exception("the file is not found .");
	        }
	    }
	    
	    public static void main(String[] args) throws IOException, ParseException,SemanticException 
	    {
	        ParseDriver pd = new ParseDriver();
	       
	       // finalResultExcel.CreateExcel(resultExcelFile);
	        TxtFile.CreateTxtFile(resultTxtFile);
	       	List<String> str1 = new ArrayList<>();
	      	try {
							
				for (int s = 0; s < SQLfiles.size(); s++) {	    	
			        String FileName = SQLfiles.get(s).toString();			        
				   	str1 = SQLFilesOperate.readFileContent(FileName);
				   	for(String list1:str1)
				   	{
					   	String parsesql = list1;
					    HiveParse hp = new HiveParse();
					    System.out.println(parsesql);
					    ASTNode ast = pd.parse(parsesql);
					    System.out.println(ast.toStringTree());
					    JoinSequence = 0;
					    hp.parse(FileName,ast);
				   	}
				   }
			       //HDFSFileUtil.UploadFile(resultTxtFile,hdfsDest+txtFileName);

		    	 
			} catch (Exception e) {
				e.printStackTrace();
			}
	       	
	        
	    }	    
}
   
