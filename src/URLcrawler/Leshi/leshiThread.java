package URLcrawler.Leshi;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import jxHan.Crawler.Util.Connection.ConnectioinFuction;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import Utils.TextValue;
import hbase.HBaseCRUD;

public class leshiThread implements Runnable {
	public static long time;
	public static ArrayList<String> urlList;
	public HBaseCRUD hbase = new HBaseCRUD();
	int step;
	public ArrayList<leshiThread> pool;
	public static Integer k;
	public JDBCConnection jdbc=new JDBCConnection();
	
	public leshiThread( ArrayList<leshiThread> pool,int step) {
		this.pool = pool;
		this.step = step;
	}

	public void play(String url, String ParentUrl) {// 电视剧动漫综艺Info页获取视频链接
		String Details = visitURL(url);
		if(Details.isEmpty()){
			jdbc.log("徐萌", url, 1, "ls", url, "url目录页连接错误", 3);
		}
		else if(Details.length()<10){
			jdbc.log("徐萌", url, 1, "ls", url, "url目录页"+Details, 3);
		}
		int Begin = Details.indexOf("julist2_child");
		int PlayExist = -1;
		if (Begin > 0)
			PlayExist = Details.indexOf("<dl class=\"w120\">", Begin);
		else
			PlayExist = Details.indexOf("<dl class=\"w120\">");
		if (PlayExist < 0)
			return;
		while (PlayExist > 0) {
			int PlayBegin = Details.indexOf("href=\"", PlayExist);
			int PlayEnd = Details.indexOf(".html\"", PlayBegin);
			String PlayUrl = "";
			if (PlayBegin > 0 && PlayEnd > 0) {
				PlayUrl = Details.substring(PlayBegin + 6, PlayEnd + 5);
			}
			else{
				break;
			}
			Details=Details.substring(PlayEnd);
			PlayExist = Details.indexOf("<dl class=\"w120\">");
			SaveAll(PlayUrl + " " + ParentUrl);
		}
	}

	public void Variety(String url, String ParentUrl) {// 综艺具体视频链接为动态，这里构成动态页面的链接
		String content = visitURL(url);
		if(content.isEmpty()){
			jdbc.log("徐萌", url, 1, "ls", url, "url目录页连接错误", 3);
		}
		else if(content.length()<10){
			jdbc.log("徐萌", url, 1, "ls", url, "url目录页"+content, 3);
		}
		int var = content.indexOf("list-month");
		if (var < 0)
			return;
		String year = "";
		String month = "";
		int pid = url.indexOf("zongyi/");
		int pidEnd = url.indexOf(".html", pid);
		String id = url.substring(pid + 7, pidEnd);
		while (var > 0) {
			month = content
					.substring(var + 12, content.indexOf("\"", var + 13));
			int yvar = content.indexOf("list-year", var);
			year = content.substring(yvar + 11,
					content.indexOf("\"", yvar + 13));
			String VideoUrl = "http://api.letv.com/mms/out/albumInfo/getVideoListByIdAndDate?callback&year="
					+ year + "&type=1&month=" + month + "&id=" + id;
			VarietyUrl(VideoUrl, ParentUrl);
			content=content.substring(yvar);
			var = content.indexOf("list-month");
		}
	}

	public void VarietyUrl(String Url, String ParentUrl) { // 从动态链接里读出视频链接
		String content = visitURL(Url);
		if(content.isEmpty()){
			jdbc.log("徐萌", Url, 1, "ls", Url, "url目录页连接错误", 3);
		}
		else if(content.length()<10){
			jdbc.log("徐萌", Url, 1, "ls", Url, "url目录页"+content, 3);
		}
		int Variety = content.indexOf("id\":");
		while (Variety > 0) {
			int VarietyEnd = content.indexOf(",", Variety);
			String ID = content.substring(Variety + 4, VarietyEnd);
			String url = "http://www.letv.com/ptv/vplay/" + ID + ".html";
			SaveAll(url + " " + ParentUrl);
			content=content.substring(VarietyEnd);
			Variety = content.indexOf("id\":");
		}
	}

	public void movieurl(String Url) {
		String content=visitURL(Url);
		if(content.isEmpty()){
			jdbc.log("徐萌", Url, 1, "ls", "", "url目录页连接错误", 3);
		}
		else if(content.length()<10){
			jdbc.log("徐萌", Url, 1, "ls", "", "url目录页"+content, 3);
		}
		Document doc1=Jsoup.parse(content);
		Elements allLink = doc1.getElementsByClass("P_t").select("a[href]");
		for (Element link : allLink) {
			if (link != null) {
				String temp = link.attr("abs:href");
				SaveAll(temp);
			}
		}
	}

	public void SaveAll(String url) {// 将获取的电视剧动漫每一集，电影播放页存入Hbase总表
		int keyS = url.indexOf("vplay/");
		int keyE = url.indexOf(".html", keyS);
		if(keyS + 6>keyE){
			System.out.println(url);
			return;
		}
		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue urltv = new TextValue();
		urltv.text = "url";
		urltv.value = "leshi " +url;
		values.add(urltv);

		TextValue keytv = new TextValue();
		keytv.text = "rowkey";
		keytv.value = url;
		values.add(keytv);

		TextValue websitetv = new TextValue();
		websitetv.text = "website";
		websitetv.value = "ls";
		values.add(websitetv);
		
		String key = url.substring(keyS + 6, keyE);	
		String[] leshirows = { key };
		String[] leshicolfams = { "C" };
		String[] leshiquals = { "url" };
		String[] leshivalues = { "leshi "+url };
		try {
			hbase.putRows("leurlnew", leshirows, leshicolfams, leshiquals,
					leshivalues);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String sd = sdf.format(new Date());
			jdbc.insert(values, "urls" + sd);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		leshirows = null;
		leshicolfams = null;
		leshiquals = null;
		leshiquals = null;
	}

	@Override
	public void run() {
		while (true) {
			String url = null;
			synchronized (urlList) {
				if (urlList != null && urlList.size() > 0) {
					url = urlList.get(0);
					urlList.remove(0);
				} else if (urlList == null || urlList.size() == 0) {
					synchronized (pool) {
						try {
							hbase.commitPuts();
						} catch (Exception e) {
							e.printStackTrace();
						}
						pool.remove(this);			
						break;
					}
				}
			}
			if (step == 1) {
				StringBuilder anti = new StringBuilder(url.substring(0,
						url.indexOf(".html")));
				anti.reverse();
				int typeBegin = url.indexOf(".com/");
				int typeEnd = url.indexOf("/", typeBegin + 5);
				String infotype = url.substring(typeBegin + 5, typeEnd);
				try{
					if (infotype.equals("tv")) {
						play(url, anti.toString());
					} else if (infotype.equals("comic")) {
						play(url, anti.toString());
					}
					else if (infotype.equals("zongyi")) {
						Variety(url, anti.toString());
					}
				}
				catch(Exception e){
				}
			} else if (step == 2) {
				try{
					movieurl(url);// 将获取的电影目录页用多线程抓取目录页上的电影url
				}
				catch(Exception e){
				}
			} 
				
			
		}
	}

	public static String visitURL(String href) {
		String content = null;
		int count = 0;
		while (true) {
			content = ConnectioinFuction.readURL(href);
			if (!content.isEmpty())
				break;
			count++;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (count == 10) {			
				break;
			}
		}
		return content;
	}
}
