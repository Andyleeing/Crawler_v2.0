package Utils;

public class SysParams {
	public static String LogTable = "Log";
	public static String urlTable_Mysql = "urls";
	public static String urlTable_Hbase_yk = "infoplayall";
	public static String urlTable_Hbase_iy = "iqiyiListAll";
	public static String urlTable_Hbase_tx = "tencentnew";
	public static String urlTable_Hbase_xl = "xunleinew";
	public static String urlTable_Hbase_ls = "leurlnew";
	public static String urlTable_Hbase_56 = "url56new";
	public static String urlTable_Hbase_sh = "urlshnew";
	//public static String urlTable_Hbase_sh = "infoplayall";
	public static String parseTable_Hbase = "parseDirsnew";
	public static int VMcount = 12;
	
	public static int timesperDay = 1;
	public static int startHour = 2;
	
	public static String urlTable_Hbase_local = "crawler-12";
	public static String urlfilePath = "src/DataCrawler/urls.txt";
	public static String locaLogfilePath = "src/DataCrawler/crawlelog.txt";
	
	public static int DataCrawlerThreadCount = 10;
	public static int URLCrawlerThreadCount = 10;
}
