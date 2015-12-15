package URLcrawler.Iqiyi;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.helper.HttpConnection;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;

public class UrlOut {
	public void DmInoutput(String strurl) throws IOException,
			InterruptedException {
		try {
			HttpConnection conn = (HttpConnection) Jsoup.connect(strurl);
			conn.timeout(10000);
			conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
			Document doc = conn.get();// doc指动漫的原网页，此时是第一页strurl。
			Elements allLinkfirst = null;
			allLinkfirst = doc.getElementsByAttributeValue("class",
					"site-piclist_info_title").select("a[href]");// 第一页每个电影的url.
			if(allLinkfirst==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("李辉", strurl+"+iy", 1, "iy", strurl, "此URL列表未能获得", 3);
				logc.closeConn();
				
			}
			for (Element linkfirst : allLinkfirst) {
				if (linkfirst != null) {
					String temfirst = linkfirst.attr("href");// 每个动漫的url。

					String urlfirst = temfirst.replaceAll("\\s*", "");
					Item.DmIntableList.add(urlfirst);
				}
			}
			Elements allLink = null;
			Elements contentPart = null;
			contentPart = doc.getElementsByAttributeValue("class", "mod-page");
			allLink = contentPart.select("a[href]");// allLink中存储每页的链接。
			for (Element link : allLink) {
				if (link != null) {
					String tempurl = link.attr("abs:href");// temp每页的全地址。
					HttpConnection conntem = (HttpConnection) Jsoup
							.connect(tempurl);
					conntem.timeout(10000);
					conntem.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
					Document doctem = conntem.get(); // 每页的源代码。
					Elements allLinktmp = null;
					allLinktmp = doctem.getElementsByAttributeValue("class",
							"site-piclist_info_title").select("a[href]");// 每个电影的url.
					if(allLinktmp==null) {
						JDBCConnection logc = new JDBCConnection();
						logc.log("李辉", tempurl+"+iy", 1, "iy", tempurl, "此URL列表未能获得", 3);
						logc.closeConn();	
					}
					
					for (Element linktmp : allLinktmp) {
						if (linktmp != null) {
							String temurl = linktmp.attr("href");// 每个动漫的url。
							String urlmod = temurl.replaceAll("\\s*", "");
							Item.DmIntableList.add(urlmod);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public void MvHoutput(String strurl) throws IOException,
	InterruptedException {
try {
	HttpConnection conn = (HttpConnection) Jsoup.connect(strurl);
	conn.timeout(10000);
	conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
	Document doc = conn.get();// doc指匹配电影片花的原网页，此时是第一页strurl。
	String fircont=doc.toString();
	int pageindex=fircont.lastIndexOf("title=\"跳转至第");
	int endpage=fircont.indexOf("页",pageindex);
   if(pageindex<endpage && pageindex>=0) {
	int page=Integer.parseInt(fircont.substring(pageindex+11, endpage));

	for(int i=1;i<=page;i++) {
		String url=strurl.substring(0,strurl.indexOf("--4-")+4)+i+"-2-iqiyi-1-.html";
		HttpConnection connect = (HttpConnection) Jsoup.connect(url);
		connect.timeout(10000);
		connect.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
		Document docc = connect.get();// doc指匹配电影片花的原网页，此时是第一页strurl。
		Elements allLinkfirst = null;
		allLinkfirst = docc.getElementsByAttributeValue("class",
				"site-piclist_info_title").select("a[href]");// 第一页每个电影的url.
	
		if(allLinkfirst==null) {
			JDBCConnection logc = new JDBCConnection();
			logc.log("李辉", url+"+iy", 1, "iy", url, "此URL列表未能获得", 3);
			logc.closeConn();
			
		}
		
		for (Element linkfirst : allLinkfirst) {
			if (linkfirst != null) {
				String temfirst = linkfirst.attr("href");// 每个电影片花的url。
				String urlfirst = temfirst.replaceAll("\\s*", "");
			
				Item.MvHtableList.add(urlfirst);
			}
		}
	}
   }
} catch (Exception e) {

	e.printStackTrace();
}
}

	
	public void Mvoutput(String strurl) throws IOException,
			InterruptedException {
		try {
			HttpConnection conn = (HttpConnection) Jsoup.connect(strurl);
			conn.timeout(100000);
			conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
			Document doc = conn.get();// doc指匹配电影的原网页，此时是第一页strurl。
			Elements allLinkfirst = null;
			allLinkfirst = doc.getElementsByAttributeValue("class",
					"site-piclist_info_title").select("a[href]");// 第一页每个电影的url.
			if(allLinkfirst==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("李辉", strurl+"+iy", 1, "iy", strurl, "此URL列表未能获得", 3);
				logc.closeConn();				
			}
			
			for (Element linkfirst : allLinkfirst) {
				if (linkfirst != null) {
					String temfirst = linkfirst.attr("href");// 每个电影的url。
					String urlfirst = temfirst.replaceAll("\\s*", "");
					Item.MvtableList.add(urlfirst);
				}
			}
			// int size=Mvm.tableList.size();

			Elements allLink = null;
			Elements contentPart = null;
			contentPart = doc.getElementsByAttributeValue("class", "mod-page");
			allLink = contentPart.select("a[href]");// allLink中存储每页的链接。

			for (Element link : allLink) {

				if (link != null) {
					String tempurl = link.attr("abs:href");// temp每页的全地址。
					HttpConnection conntem = (HttpConnection) Jsoup
							.connect(tempurl);
					conntem.timeout(10000);
					conntem.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
					Document doctem = conntem.get(); // 每页的源代码。
					// Elements contentparttmp=null;
					Elements allLinktmp = null;
					allLinktmp = doctem.getElementsByAttributeValue("class",
							"site-piclist_info_title").select("a[href]");// 每个电影的url.
					if(allLinktmp==null) {
						JDBCConnection logc = new JDBCConnection();
						logc.log("李辉", tempurl+"+iy", 1, "iy", tempurl, "此URL列表未能获得", 3);
						logc.closeConn();				
					}					
					for (Element linktmp : allLinktmp) {
						if (linktmp != null) {
							String temurl = linktmp.attr("href");// 每个电影的url。
							String urlmod = temurl.replaceAll("\\s*", "");
							Item.MvtableList.add(urlmod);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void TvInoutput(String strurl) throws IOException,
			InterruptedException {
		try {
			HttpConnection conn = (HttpConnection) Jsoup.connect(strurl);
			conn.timeout(10000);
			conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
			Document doc = conn.get();// doc指匹配电视剧的原网页，此时是第一页strurl。
			Elements allLinkfirst = null;
			allLinkfirst = doc.getElementsByAttributeValue("class",
					"site-piclist_info_title").select("a[href]");// 第一页每个电影的url.
			if(allLinkfirst==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("李辉", strurl+"+iy", 1, "iy", strurl, "此URL列表未能获得", 3);
				logc.closeConn();				
			}
			
			for (Element linkfirst : allLinkfirst) {
				if (linkfirst != null) {
					String temfirst = linkfirst.attr("href");// 每个电视剧的url。

					String urlfirst = temfirst.replaceAll("\\s*", "");
					Item.TvIntableList.add(urlfirst);
				}
			}
			Elements allLink = null;
			Elements contentPart = null;
			contentPart = doc.getElementsByAttributeValue("class", "mod-page");
			allLink = contentPart.select("a[href]");// allLink中存储每页的链接。
			for (Element link : allLink) {

				if (link != null) {
					String tempurl = link.attr("abs:href");// temp每页的全地址。
					HttpConnection conntem = (HttpConnection) Jsoup
							.connect(tempurl);
					conntem.timeout(10000);
					conntem.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
					Document doctem = conntem.get(); // 每页的源代码。
					Elements allLinktmp = null;
					allLinktmp = doctem.getElementsByAttributeValue("class",
							"site-piclist_info_title").select("a[href]");// 每个电影的url.
					if(allLinktmp==null) {
						JDBCConnection logc = new JDBCConnection();
						logc.log("李辉", tempurl+"+iy", 1, "iy", tempurl, "此URL列表未能获得", 3);
						logc.closeConn();				
					}
					
					
					for (Element linktmp : allLinktmp) {
						if (linktmp != null) {
							String temurl = linktmp.attr("href");// 每个电视剧的url。
							String urlmod = temurl.replaceAll("\\s*", "");
							Item.TvIntableList.add(urlmod);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void ZyInoutput(String strurl) throws IOException,
			InterruptedException {
		try {
			HttpConnection conn = (HttpConnection) Jsoup.connect(strurl);
			conn.timeout(100000);
			conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
			Document doc = conn.get();// doc指匹配电影的原网页，此时是第一页strurl。
			Elements allLinkfirst = null;
			allLinkfirst = doc.getElementsByAttributeValue("class",
					"site-piclist_info_title").select("a[href]");// 第一页每个电影的url.
			if(allLinkfirst==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("李辉", strurl+"+iy", 1, "iy", strurl, "此URL列表未能获得", 3);
				logc.closeConn();				
			}
			
			
			for (Element linkfirst : allLinkfirst) {
				if (linkfirst != null) {
					String temfirst = linkfirst.attr("href");// 每个综艺的url。

					String urlfirst = temfirst.replaceAll("\\s*", "");
					Item.ZyIntableList.add(urlfirst);
				}
			}

			Elements allLink = null;
			Elements contentPart = null;
			contentPart = doc.getElementsByAttributeValue("class", "mod-page");
			allLink = contentPart.select("a[href]");// allLink中存储每页的链接。

			for (Element link : allLink) {

				if (link != null) {
					String tempurl = link.attr("abs:href");// temp每页的全地址。
					HttpConnection conntem = (HttpConnection) Jsoup
							.connect(tempurl);
					conntem.timeout(50000);
					conntem.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
					Document doctem = conntem.get(); // 每页的源代码。
					Elements allLinktmp = null;
					allLinktmp = doctem.getElementsByAttributeValue("class",
							"site-piclist_info_title").select("a[href]");// 每个电影的url.
					if(allLinktmp==null) {
						JDBCConnection logc = new JDBCConnection();
						logc.log("李辉", tempurl+"+iy", 1, "iy", tempurl, "此URL列表未能获得", 3);
						logc.closeConn();				
					}
										
					for (Element linktmp : allLinktmp) {
						if (linktmp != null) {
							String temurl = linktmp.attr("href");// 每个电视剧的url。
							String urlmod = temurl.replaceAll("\\s*", "");
							Item.ZyIntableList.add(urlmod);
						}
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
