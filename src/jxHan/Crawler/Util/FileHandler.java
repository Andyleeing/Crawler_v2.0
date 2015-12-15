package jxHan.Crawler.Util;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import jxHan.Crawler.Util.Log.ExceptionHandler;

public class FileHandler {
	private static String filepath;
	public static File file;
	public static FileWriter fileWritter = null;
	public static BufferedWriter bufferWritter = null;
	public FileHandler(String filepath) {
		// TODO Auto-generated constructor stub
	}
	public static void setFilePath(String newfilepath) {
		filepath = newfilepath;
		file = null;
		fileWritter = null;
		bufferWritter = null;
	}
	public static void writeSaveUrl(String url) {
		if(file == null)
		file = new File(filepath);
		try {
			if (!file.exists()) {
				File parent = file.getParentFile(); 
				if(parent!=null&&!parent.exists()){ 
					parent.mkdirs(); 
				}
					file.createNewFile();
			}
			if(fileWritter == null)
			fileWritter = new FileWriter(file,true);
			if(bufferWritter == null)
			bufferWritter = new BufferedWriter(fileWritter);
			bufferWritter.write(url+"\n");
			bufferWritter.flush();
		}catch(Exception e) {
			ExceptionHandler.log(url, e);
		}
	}
	
	public static void endWrite() {
		
		try {
			if(bufferWritter != null)
			bufferWritter.close();
			if(fileWritter != null)
			fileWritter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			ExceptionHandler.log("endWrite", e);
		}
		file = null;
		fileWritter = null;
		bufferWritter = null;
		
	}
}
