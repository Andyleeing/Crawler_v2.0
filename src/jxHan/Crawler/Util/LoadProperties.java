package jxHan.Crawler.Util;



import java.io.*;
import java.util.Properties;

import jxHan.Crawler.Util.Log.ExceptionHandler;

public class LoadProperties {
	 private static String CONNPOOLMGRPROPATH = "conPoolMgrProConfig.properties";
	 
	 private static Properties connPoolMgrPro = null;
	 public static Properties getConnPoolMgrPro() {
		 	if(connPoolMgrPro != null)
		 		return connPoolMgrPro;
	        InputStream in;
	        connPoolMgrPro = new Properties();
			try {
				in = new BufferedInputStream (new FileInputStream(CONNPOOLMGRPROPATH));
				connPoolMgrPro.load(in);
				in.close();
			} catch (FileNotFoundException e) {
				ExceptionHandler.log("Could not find the file!", e);
				//System.out.println("Could not find the file!");
			} catch (IOException e) {
				ExceptionHandler.log("IO exception!", e);
			} catch (NullPointerException e) {
				ExceptionHandler.log("Nothing in this file!", e);
			}
		 return connPoolMgrPro;
	 }
	 public static Properties loadWebURLPro(String filepath) {
		 Properties file = new Properties();
		 InputStream in;
		 try {
				in = new BufferedInputStream (new FileInputStream(filepath));
				file.load(in);
				in.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Could not find the file!");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("IO exception!");
			} catch (NullPointerException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				System.out.println("Nothing in this file!");
			}
		 return file;
	 }
}
