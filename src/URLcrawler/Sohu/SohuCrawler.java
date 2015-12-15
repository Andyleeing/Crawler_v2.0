package URLcrawler.Sohu;


import hbase.HBaseCRUD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import jxHan.Crawler.Util.Log.ExceptionHandler;

public class SohuCrawler {
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static int threadCount = 10;

	public static void sohuUrlCrawler(Date date) {
		SohuUrl(date);
//		Utils.createFile("url/sohu1.txt");
//		try {
//			Utils.readTables();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	private static void Urls(Date date) {
		Utils.crawlYD();
		Utils.crawlZongyi();
		Utils.crawlDongman();
	}
	private static void SohuUrl(Date date) {

		ExceptionHandler.filepath = "src/sohu/exception"
				+ System.currentTimeMillis() + ".txt";
		ArrayList<CrawlerThread> pool = new ArrayList<CrawlerThread>();
		Urls(date);
		Iterator<String> ite = CrawlerThread.urlList.iterator();
		System.out.println("urllist-->"+CrawlerThread.urlList.size());
		String item;
		while (ite.hasNext()) {
			item = ite.next();
			CrawlerThread.arrList.add(item);
		}
		int threadCount = 10;
		for (int i = 0; i < threadCount; i++) {
			CrawlerThread crawlerthread = new CrawlerThread(date.getTime(),
					date.toString(), pool);
			pool.add(crawlerthread);
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(crawlerthread).start();
		}
		while (true) {
			synchronized (pool) {
				if (pool.size() <= 0||CrawlerThread.arrList.size()<=0)
					break;
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		Utils.jdbc.closeConn();
		System.out.println("pool end");
	}
	
}

