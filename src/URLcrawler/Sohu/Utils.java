package URLcrawler.Sohu;
import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import Utils.JDBCConnection;
import Utils.SysParams;
import jxHan.Crawler.Util.Connection.ConnectionPool;
import jxHan.Crawler.Util.Log.ExceptionHandler;

public class Utils {
	public static HBaseCRUD hbase=new HBaseCRUD();
	public static String TABLE=SysParams.urlTable_Hbase_local;
	public static JDBCConnection jdbc=new JDBCConnection();
	
	public static void putSohuUrl(String key,String value){
		if(value.indexOf("sohu.com")<0)return;
		String infoKey = key;
		String[] sohurows = { infoKey };
		String[] sohucolfams = { "C" };
		String[] sohuquals = { "url" };
		String[] sohuvalues = {"sohu "+value};
		try {
			hbase.putRows(SysParams.urlTable_Hbase_sh, sohurows, sohucolfams, sohuquals,
					sohuvalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		mysqlUrl(value);
	}
	
  public static void deleteTable(){
	  String[]columns={"C"};
	  try {
		hbase.dropTable("zl");
		hbase.createHTable("zl", columns);
	} catch (MasterNotRunningException e) {
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
	public static void createFile(String filepath) {
		File file = new File(filepath);
		File parent = file.getParentFile();
		if (parent != null && !parent.exists()) {
			parent.mkdirs();
		}
		try {
			file.createNewFile();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		FileWriter fw;
		try {
			fw = new FileWriter(file);
			fw.write("");
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	public static void crawlEpisode(){
		// episode
		String url=null;
		String areas[] = { "_u5185_u5730", "_u6e2f_u5267", "_u53f0_u5267",
				"_u7f8e_u5267", "_u97e9_u5267", "_u82f1_u5267", "_u6cf0_u5267",
				"_u65e5_u5267", "_u5176_u4ed6" };
		String types[] = { "101100", "101101", "101102", "101103", "101104",
				"101105", "101106", "101107", "101108", "101109", "1011010",
				"1011011", "1011012", "1011013", "1011014", "1011015",
				"1011016", "1011017", "1011018", ""
						+ "", "1011020",
				"1011021", "1011022", "1011023", "1011024", "1011027" };
		 for (int i = 0; i < types.length; i++) {
		 for (int j = 0; j < areas.length; j++) {
		 int page = 1;
		 while (page <= 20) {
		 url = "http://so.tv.sohu.com/list_p1101_p2" + types[i]
		 + "_p3" + areas[j]
		 + "_p40_p5_p6_p73_p80_p9_2d3_p10" + page
		 + "_p11.html";
		 if (url == null || url.equals(""))
		 continue;
		 String content = null;
		 int index = 0;
		 String infoUrl=null;
		 content = Utils.visitUrl(url);
		 if (content == null || content.equals("")) break;
		 index = content.indexOf("rl-phua");
			if (index < 0)
				break;
			int indexAnother=0;
			while (index > -1) {
				content = content.substring(index + 6);
				index=content.indexOf("href") + 6;
				indexAnother=content.indexOf("\" title");
				if(index<0||indexAnother<0)break;
				if(index<0||indexAnother<0)break;
				infoUrl = content.substring(index,indexAnother);
				if (CrawlerThread.urlList.add(infoUrl) && !infoUrl.equals("")
						&& infoUrl != null) {
					if (infoUrl.indexOf("com/") < 0)
						break;
					String key = infoUrl.substring(infoUrl.indexOf("com/") + 4);
//					printData(infoUrl+"@yingshi@info");
					// Utils.putSohuUrl(key,infoUrl+"@"++"@info");
				}
				index = content.indexOf("rl-phua");
			}
			page++;
		 }
		 }
		 }
	}
	
	public static void appendFile(String fileName, String content) {
	try {
		FileWriter fw = new FileWriter(fileName, true);
		fw.write(content);
		fw.close();
	} catch (IOException e) {
		e.printStackTrace();
	}
}
	public static void readTables() throws IOException {
	int count = 0;
	ResultScanner res = hbase.queryAll("sohuurl");
	Iterator<Result> ite = res.iterator();
	while (ite.hasNext()) {
		String urlString = "";
		Result r = ite.next();
		byte[] url = r.getValue("C".getBytes(), "url".getBytes());
		if (url == null)continue;
		urlString = new String(url, "utf-8");
		System.out.println(urlString);
		appendFile("url/sohu1.txt", urlString + "\n");
		mysqlUrl(urlString);
		count++;
	}
	System.out.println("count" + count);
}
	
	public static void mysqlUrl(String url){
		int index=0;
		index=url.indexOf("@");
		if(index<0)return;
		url=url.substring(0,index);
		Date date=new Date();
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		String sd=sdf.format(date);
		String tablename="urls"+sd;
		System.out.println(tablename);
		index=url.indexOf("com/");
		String rowkey ;
		if(index<0)return;
		rowkey=url.substring(index+4);
		String sql="insert into "+tablename+"(rowkey,url,website)values('"+rowkey+"','"+url+"','sh')";
		jdbc.update(sql);
	}
	public static void crawlYD(){
		String url=null;
		String[] years = {"2015", "2014", "2013", "2012", "2011", "2010", "11", "90",
				"80", "1" };
		String[] cut1 = { "http://so.tv.sohu.com/list_p1101_p20_p3_p4",
				"http://so.tv.sohu.com/list_p1100_p20_p3_p4" };
		String[] cut2 = { "_p5_p6_p75_p80_p9_2d1_p10",
				"_p5_p6_p77_p80_p9_2d0_p10" };
		String[] category = { "yingshi", "dianying" };
		for (int j = 0; j <2; j++) {
			for (int count = 0; count < years.length; count++) {
				String year = years[count];
				int page = 1;
				while (page <= 200) {
					url = cut1[j] + year + cut2[j] + page + "_p11.html";
					if (url == null || url.equals(""))
						continue;
					String content = null;
					int index = 0;
					content = Utils.visitUrl(url);
					if (content == null || content.equals(""))
						continue;
					if(j==1)
					index = content.indexOf("st-msk");
					else if(j==0)
					index=content.indexOf("maskTx");
					if (index < 0)
						break;
					String infoUrl = null;
					while (index > -1) {
						content = content.substring(index + 6);
						if(content.indexOf("href")<0||content.indexOf("html")<0)break;
						infoUrl = content.substring(
								content.indexOf("href") + 6,
								content.indexOf("html") + 4);
						if (CrawlerThread.urlList.add(infoUrl+"@"+category[j]+"@info")
								&& !infoUrl.equals("") && infoUrl != null) {
							if (infoUrl.indexOf("com/") < 0)
								break;
							String key = infoUrl.substring(infoUrl
									.indexOf("com/") + 4);
							 Utils.putSohuUrl(key,infoUrl+"@"+category[j]+"@info");
						}
						if(j==1)
						index = content.indexOf("st-msk");
						else if(j==0)
						index=content.indexOf("maskTx");
					}
					page++;
				}
			}
		}
	}
//	public static void printData(String value){
//		System.out.println(value);
//		try {
//			CrawlerThread.crawlurl.write(value+"\n");
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//	}
	public static void crawlZongyi() {
		int pageTop = 60;
		int page = 1;
		String url = null;
		String content = null;
		int index = 0;
		String infoUrl = null;
		for (int j = 0; j < pageTop; j++) {
			url = "http://so.tv.sohu.com/list_p1106_p2_p3_p4_p5_p6_p7_p8_p9_p10"
					+ page + "_p11_p12_p13.html";
			content = Utils.visitUrl(url);
			if (content == null || content.equals(""))
				continue;
			index = content.indexOf("maskTx");
			if (index < 0)
				break;
			int indexAnother=0;
			while (index > -1) {
				content = content.substring(index + 6);
				index=content.indexOf("href") + 6;
				indexAnother=content.indexOf("\" title");
				if(index<0||indexAnother<0)break;
				if(index<0||indexAnother<0)break;
				infoUrl = content.substring(index,indexAnother);
				if (CrawlerThread.urlList.add(infoUrl+"@zongyi@info") && !infoUrl.equals("")
						&& infoUrl != null) {
					if (infoUrl.indexOf("com/") < 0)
						break;
					String key = infoUrl.substring(infoUrl.indexOf("com/") + 4);
//					printData(infoUrl+"@zongyi@info");
					 Utils.putSohuUrl(key,infoUrl+"@zongyi@info");
				}
				index = content.indexOf("maskTx");
			}
			page++;
		}
	}
	public static void crawlDongman(){
		int pageTop = 60;
		int page = 1;
		String url = null;
		String content = null;
		int index = 0;
		int indexAnother=0;
		String infoUrl = null;
		for (int j = 0; j < pageTop; j++) {
			int countPage=0;
			url =  "http://so.tv.sohu.com/list_p1115_p2_p3_p4_p5_p6_p7_p8_p9_p10"+page+"_p11_p12_p13.html";
			content = Utils.visitUrl(url);
			if (content == null || content.equals(""))
				continue;
			index = content.indexOf("maskTx");
			if (index < 0)
				break;
			while (index > -1) {
				content = content.substring(index + 6);
				index=content.indexOf("href") + 6;
				indexAnother=content.indexOf("\" title");
				if(index<0||indexAnother<0)break;
				infoUrl = content.substring(index,indexAnother);
				if (CrawlerThread.urlList.add(infoUrl+"@dongman@info") && !infoUrl.equals("")
						&& infoUrl != null) {
					if (infoUrl.indexOf("com/") < 0)
						break;
					String key = infoUrl.substring(infoUrl.indexOf("com/") + 4);

					 Utils.putSohuUrl(key,infoUrl+"@dongman@info");
					 countPage++;
				}
				index = content.indexOf("maskTx");
			}
			page++;

		}
	}
	public static void crawlPaid(){
		int page=1;
		String startUrl=null;
		String content=null;
		 int count=0;
		while(page<=50){
			startUrl="http://store.tv.sohu.com/web/search/show.do?categoryId=1&contCates=-1&buyType=-1&year=-1&areaId=-1&sortField=1&producer=-1&totalProducer=-1&tyFileArea=-1&pageNo="+page+"&limit=20";
		    content=visitUrl(startUrl);
		    Pattern p=Pattern.compile("vlist_lipic.{10,100}target");
		    Matcher m=p.matcher(content);
		    while(m.find()){
		        String subString=m.group();
		        Pattern p2=Pattern.compile("http://.*shtml");
		        Matcher m2=p2.matcher(subString);
		        while (m2.find()) {
		        	String findUrl=m2.group();
		        	if (CrawlerThread.urlList.add(findUrl+"@paid") && !findUrl.equals("")
							&& findUrl != null) {
						if (findUrl.indexOf("com/") < 0)break;
						String key = findUrl.substring(findUrl.indexOf("com/") + 4);
						 Utils.putSohuUrl(key,findUrl+"@paid");
					}
					System.out.println(m2.group()+"--"+count++);
				}
		    }
			if(content==null||content.equals(""))continue;
			page++;
		}
	}
	
	public static String visitUrl(String url) {
		 URL obj=null;
		 URLConnection conn=null;
		try {
			obj = new URL(url);
			conn = obj.openConnection();
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
       Map<String, List<String>> map = conn.getHeaderFields();
       Set<String> keys = map.keySet();
       Iterator<String> iterator = keys.iterator();

       String key = null;
       String tmp = null;
       String strencoding=null;
       while (iterator.hasNext()) {
           key = iterator.next();
           tmp = map.get(key).toString().toLowerCase();
           if (key != null && key.equals("Content-Type")) {
               int m = tmp.indexOf("charset=");
               if (m != -1) {
                   strencoding = tmp.substring(m + 8).replace("]", "");
                   break;
               }
           }
       }

       if(strencoding==null||strencoding.isEmpty())strencoding="gbk";
		HttpResponse response = null;
		StringBuffer pageBuffer = new StringBuffer();
		BufferedReader reader = null;
		try {
			HttpGet get = new HttpGet(url);
			response = ConnectionPool.getDefaultHttpClient().execute(get);
		
			if(response==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("郑玲", url+"+sh", 1, "sh", url, "此URL列表未能获得", 3);
				logc.closeConn();                             //记录日志
			}

			if(response.getStatusLine().getStatusCode() != 200) {
				Thread.sleep(500);
			}
			reader = new BufferedReader(new InputStreamReader(
					response.getEntity().getContent(),strencoding));
			String line;
			
			while ((line = reader.readLine()) != null) {
				pageBuffer.append(line);				
			}
			
		
		}catch (Exception e) {
			ExceptionHandler.log(url, e);//Exception
		}finally {
			try {
				if(reader != null)
				reader.close();
			} catch (IOException e) {
				ExceptionHandler.log(url, e);
			}
		}
	
		return pageBuffer.toString();
	}
		public static HttpGet initHttpGet(String url) {
			HttpGet get = new HttpGet(url);

			get.addHeader(
					"User-Agent",
					"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.160 Safari/537.22");
			return get;
		}
}
