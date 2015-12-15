package URLcrawler.Youku;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import jxHan.Crawler.Util.XML.URLmaker;
import jxHan.Crawler.Util.XML.XMLfunction;
import jxHan.Crawler.WebSite.Base.GlobalData;
import Utils.JDBCConnection;
import Utils.SysParams;

public class YoukuURLCrawler {

	public void crawler() {

		JDBCConnection conn1 = new JDBCConnection();
		String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('"
				+ SysParams.urlTable_Hbase_local
				+ "','1',now(),'抓取URL开始','yk','韩江雪')";
		conn1.update(sql1);
		conn1.closeConn();
		conn1 = null;

		Date date = new Date();
		// /////movie

		GlobalData.baseURL = "http://www.youku.com/v_olist/c_96_g__a_";
		GlobalData.urlParams = XMLfunction
				.urlParams("src/URLcrawler/Youku/Youku/movie/params.xml");
		HashSet<String> urls = new HashSet<String>();
		URLmaker.makeURL(0, null, urls);
		ArrayList<String> urlList = new ArrayList<String>();
		urlList.addAll(urls);
		ArrayList<URLcrawler.Youku.Youku.movie.ListCrawlerThread> pool = new ArrayList<URLcrawler.Youku.Youku.movie.ListCrawlerThread>();
		URLcrawler.Youku.Youku.movie.ListCrawlerThread.pool = pool;
		URLcrawler.Youku.Youku.movie.ListCrawlerThread.urlList = urlList;
		for (int i = 0; i < SysParams.URLCrawlerThreadCount; i++) {
			URLcrawler.Youku.Youku.movie.ListCrawlerThread crawlerthread = new URLcrawler.Youku.Youku.movie.ListCrawlerThread(
					date.getTime(), date.toString(), 1);
			pool.add(crawlerthread);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(crawlerthread).start();
		}
		while (true) {
			synchronized (pool) {
				if (pool.size() <= 0) {
					break;
				}
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		URLcrawler.Youku.Youku.movie.ListCrawlerThread.infoplayList.clear();
		URLcrawler.Youku.Youku.movie.ListCrawlerThread.viewyouku2List.clear();
		// /////movie end ///////////////////////

		// /////TV
		GlobalData.baseURL = "http://www.youku.com/v_olist/c_97_g_";
		GlobalData.urlParams = XMLfunction.urlParams("src/URLcrawler/Youku/Youku/tv/params.xml");
		HashSet<String> TVurls = new HashSet<String>();
		URLmaker.makeURL(0, null, TVurls);
		ArrayList<String> TVurlList = new ArrayList<String>();
		TVurlList.addAll(TVurls);
		ArrayList<URLcrawler.Youku.Youku.tv.ListCrawlerThread> TVpool = new ArrayList<URLcrawler.Youku.Youku.tv.ListCrawlerThread>();
		URLcrawler.Youku.Youku.tv.ListCrawlerThread.pool = TVpool;
		URLcrawler.Youku.Youku.tv.ListCrawlerThread.urlList = TVurlList;
		for (int i = 0; i < SysParams.URLCrawlerThreadCount; i++) {
			URLcrawler.Youku.Youku.tv.ListCrawlerThread crawlerthread = new URLcrawler.Youku.Youku.tv.ListCrawlerThread(
					date.getTime(), date.toString(), 1);

			TVpool.add(crawlerthread);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(crawlerthread).start();
		}
		while (true) {
			synchronized (TVpool) {
				if (TVpool.size() <= 0) {
					System.out.println("tv over at :" + new Date().toString());
					break;
				}
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		URLcrawler.Youku.Youku.tv.ListCrawlerThread.infoplayList.clear();
		URLcrawler.Youku.Youku.tv.ListCrawlerThread.viewyoukuList.clear();
		// /////TV end ///////////////////////

		// ///////dongman//////////////////
		GlobalData.baseURL = "http://www.youku.com/v_olist/c_100_g__a_";
		GlobalData.urlParams = XMLfunction
				.urlParams("src/URLcrawler/Youku/Youku/dongman/params.xml");
		HashSet<String> DMurls = new HashSet<String>();
		URLmaker.makeURL(0, null, DMurls);
		ArrayList<String> DMurlList = new ArrayList<String>();
		DMurlList.addAll(DMurls);
		ArrayList<URLcrawler.Youku.Youku.dongman.ListCrawlerThread> DMpool = new ArrayList<URLcrawler.Youku.Youku.dongman.ListCrawlerThread>();
		URLcrawler.Youku.Youku.dongman.ListCrawlerThread.pool = DMpool;
		URLcrawler.Youku.Youku.dongman.ListCrawlerThread.urlList = DMurlList;
		for (int i = 0; i < SysParams.URLCrawlerThreadCount; i++) {
			URLcrawler.Youku.Youku.dongman.ListCrawlerThread crawlerthread = new URLcrawler.Youku.Youku.dongman.ListCrawlerThread(
					date.getTime(), date.toString(), 1);

			DMpool.add(crawlerthread);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(crawlerthread).start();
		}
		while (true) {
			synchronized (DMpool) {
				if (DMpool.size() <= 0) {
					System.out.println("a loop over at :"
							+ new Date().toString());
					break;
				}
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		URLcrawler.Youku.Youku.dongman.ListCrawlerThread.infoplayList.clear();
		URLcrawler.Youku.Youku.dongman.ListCrawlerThread.viewyouku1List.clear();
		// ////////////dongman end////////////////////////
		// ///////zongyi//////////////////
		GlobalData.baseURL = "http://www.youku.com/v_olist/c_85_g__a_";
		GlobalData.urlParams = XMLfunction
				.urlParams("src/URLcrawler/Youku/Youku/zongyi/params.xml");
		HashSet<String> ZYurls = new HashSet<String>();
		URLmaker.makeURL(0, null, ZYurls);
		ArrayList<String> ZYurlList = new ArrayList<String>();
		ZYurlList.addAll(ZYurls);
		ArrayList<URLcrawler.Youku.Youku.zongyi.ListCrawlerThread> ZYpool = new ArrayList<URLcrawler.Youku.Youku.zongyi.ListCrawlerThread>();
		URLcrawler.Youku.Youku.zongyi.ListCrawlerThread.pool = ZYpool;
		URLcrawler.Youku.Youku.zongyi.ListCrawlerThread.urlList = ZYurlList;
		for (int i = 0; i < SysParams.URLCrawlerThreadCount; i++) {
			URLcrawler.Youku.Youku.zongyi.ListCrawlerThread crawlerthread = new URLcrawler.Youku.Youku.zongyi.ListCrawlerThread(
					date.getTime(), date.toString(), 1);

			ZYpool.add(crawlerthread);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(crawlerthread).start();
		}
		while (true) {
			synchronized (ZYpool) {
				if (ZYpool.size() <= 0) {
					System.out.println("a loop over at :"
							+ new Date().toString());
					break;
				}
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		URLcrawler.Youku.Youku.zongyi.ListCrawlerThread.infoplayList.clear();
		URLcrawler.Youku.Youku.zongyi.ListCrawlerThread.viewyoukuList.clear();
		// ////////////zongyi end////////////////////////

		JDBCConnection conn11 = new JDBCConnection();
		String sql11 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.155+Crawl-11','1',now(),'抓取URL结束','yk','韩江雪')";
		conn11.update(sql11);
		conn11.closeConn();
		conn11 = null;
	}
}
