package jxHan.Crawler.WebSite.Base;

import java.util.HashSet;

public class GlobalData {
	public static long homepageCrawlerTime = 0;
	public static String baseURL;
	public static String target;
	public static String folder;
	public static String WebSite;
	public static String category;
	public static String filepath;
	public static String[][] urlParams;
	public static int paramNum;
	public static int paramMaxCount;
	public static HashSet<String> allurls=new HashSet<String>();
	public static HashSet<String> middleurls=new HashSet<String>();
	public static HashSet<String> saveurls=new HashSet<String>();

}
