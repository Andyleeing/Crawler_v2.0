package URLcrawler.Xunlei;

import hbase.HBaseCRUD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import Utils.TextValue;

public class MovieUrlList implements Runnable {
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static HashMap<String, String> map = new HashMap<String, String>();
	public String topid = null;
	public String movieid = null;
	public String subid = null;
	public String sourcedata = null;
	public String basedata = null;
	public String urlContent = null;
	private long time;
	public int i = 100;
	private String date;
	public static ArrayList<String> urlList;
	public static ArrayList<MovieUrlList> pool;
	public JDBCConnection jdbconn = new JDBCConnection();

	public MovieUrlList(long time, String date) {
		this.time = time;
		this.date = date;
	}

	public void urlDeepSearch(String strUrl) {

		try {
			// File f = new File("D://URL" + date + ".txt");
			// OutputStream out = new FileOutputStream(f,true);
			DocumetContent docContent = new DocumetContent(jdbconn);
			Document doc = docContent.getDocument(strUrl);

			Elements allLink = null;
			Element featureContent = null;
			if (doc != null) {
				featureContent = doc.getElementById("feature_container");
			}

			if (featureContent != null) {
				allLink = featureContent.getElementsByClass("pic").select(
						"a[href]");// allLink中存储每页中各个电影的链接。
				for (Element link : allLink) {
					if (link != null) {
						String subMovieUrl = link.attr("abs:href");
						String subTitle = link.attr("title");
						int vipIndex = subMovieUrl.indexOf("vip");

						if (vipIndex != 7) {
							movieid = getTopId(subMovieUrl);
							subid = getMovieId(subMovieUrl);
							if (map.containsKey(movieid + "-" + subid) == false) {
								map.put(movieid + "-" + subid, "");
								// System.out.println(subMovieUrl);
								save(subMovieUrl + "@" + "<*TY->电影<-TY*>"+
										"<*ST->" + subTitle + "<-ST*>");
								// out.write((subMovieUrl + "@" + "<*ST->" +
								// subTitle + "<-ST*>" + "<*TY->电影<-TY*>" +
								// "\r\n").getBytes());
							}
						}

					}
					// else
					// System.out.println("无分集"+strUrl);

				}
			}
		}// try
		catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			System.out.println("urlDeepSearch" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 获取迅雷看看电影的movieId
	 */
	public String getTopId(String strUrl) {
		try {
			int sub1 = strUrl.lastIndexOf('/');

			String movieIdTemp = strUrl.substring(0, sub1);
			int sub2 = movieIdTemp.lastIndexOf('/');
			String movieId = movieIdTemp.substring(sub2 + 1);
			return movieId;
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * 
	 * 获取迅雷看看电影的subId
	 */
	public String getMovieId(String strUrl) {
		try {
			int sub1 = strUrl.lastIndexOf('/');
			String subIdTemp = strUrl.substring(sub1 + 1);
			int sub3 = subIdTemp.indexOf(".shtml");
			String subId = subIdTemp.substring(0, sub3);
			return subId;
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

public void save(String url) {
		
	String key = movieid + "-" + subid  + "+xl";
		
		Date tabledate = new Date();

		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue urltv = new TextValue();
		urltv.text = "url";
		urltv.value = "xunlei " +url;
		values.add(urltv);

		TextValue keytv = new TextValue();
		keytv.text = "rowkey";
		keytv.value = key;
		values.add(keytv);

		TextValue websitetv = new TextValue();
		websitetv.text = "website";
		websitetv.value = "xl";
		values.add(websitetv);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date());
		jdbconn.insert(values, "urls" + sd);

		String[] xunleirows = { key};
		String[] xunleicolfams = { "C"};
		String[] xunleiquals = { "url" };
		 String[] xunleivalues = { "xunlei " + url };
		
		 try {
		 hbase.putRows("xunleinew", xunleirows, xunleicolfams, xunleiquals,
		 xunleivalues);
		 } catch (Exception e) {
		 // TODO Auto-generated catch block
		 e.printStackTrace();
		 }

	}

	@Override
	public void run() {
		while (true) {
			String url = "";
			synchronized (urlList) {
				if (urlList != null && urlList.size() > 0) {
					url = urlList.get(0);
					urlList.remove(0);
				} else if (urlList == null || urlList.size() == 0) {
					synchronized (pool) {
						this.jdbconn.closeConn();
						pool.remove(this);
					}
					break;
				}
			}
			// String label = getLabel(url);
			urlDeepSearch(url);

		}
	}

	public String getLabel(String str) {
		String label = null;
		int index = str.indexOf("@");
		if (index > 0)
			label = str.substring(index);
		return label;

	}

}
