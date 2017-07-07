package com.qianjiali.hiveDependency.hiveDependency;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class HDFSFileUtil {

	
	   FileSystem fileSystem = null;
	  
	   public HDFSFileUtil(String hdfsConfigPath) throws Exception {
	        try {
	            fileSystem = FileSystem.get(getConfiguration(hdfsConfigPath));
	        } catch (Exception e) {
	            throw e;
	        }
	    }
	 
	    public static Configuration getConfiguration(String hdfsConfigPath) throws MalformedURLException {
	        Configuration conf = new Configuration();
	        for (String resource : hdfsConfigPath.split(",")) {
	            conf.addResource(new URL(resource));
	        }

	        return conf;
	    }
	    
	    public List<Path> listFile(Path root) throws FileNotFoundException, IOException {
	        List<Path> fileList = new ArrayList<Path>();

	        if (!fileSystem.exists(root)) {
	            return fileList;
	        }
	        if (fileSystem.isFile(root)) {
	            fileList.add(root);
	            return fileList;
	        } else {
	            RemoteIterator<LocatedFileStatus> subFiles = fileSystem.listFiles(root, true);
	            while (subFiles.hasNext()) {
	            	Path path = subFiles.next().getPath();
	                if(fileSystem.isFile(path)){
	                	fileList.add(path);
	                }
	            }
	            return fileList;
	        }
	    }
	    
	    public static String httpGetFile(String urlPath, String dest) {
	        // TODO Auto-generated method stub
	     try{   
	        HttpClient client = new DefaultHttpClient();  
	        HttpGet httpget = new HttpGet(urlPath);  
	        HttpResponse response = client.execute(httpget);  

	        HttpEntity entity = response.getEntity();  
	        InputStream is = entity.getContent();  	    
	            
	        File file = new File(dest);	       
	        file.getParentFile().mkdirs();	        
	        FileOutputStream fileout = new FileOutputStream(file);  
	        /** 
	         * 根据实际运行效果 设置缓冲区大小 
	         */  
	        byte[] buffer=new byte[10*1024];  
	        int ch = 0;  
	        while ((ch = is.read(buffer)) != -1) {
	            fileout.write(buffer,0,ch);  
	        }  
	        is.close();  
	        fileout.flush();  
	        fileout.close();  

	    } catch (Exception e) {  
	        e.printStackTrace();  
	    }          
	     
	        return null;
	    }
	    public static String UploadFile(String urlPath, String dest) {
	       
	     try{  
	    	
	    	 InputStream in = new BufferedInputStream(new FileInputStream(urlPath));
	    	 
	    	 Configuration conf = new Configuration(); 
	         FileSystem fs = FileSystem.get(URI.create(dest), conf);
	         OutputStream out = fs.create(new Path(dest), new Progressable() {
	        	   public void progress() {
	        	    System.out.print(".");
	        	   }
	        	  });
	        	  IOUtils.copyBytes(in, out, 4096, true);

	    } catch (Exception e) {  
	        e.printStackTrace();  
	    }          
	     
	        return null;
	    }
	    
}
