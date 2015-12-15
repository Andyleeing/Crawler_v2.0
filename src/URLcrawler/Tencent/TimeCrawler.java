package URLcrawler.Tencent;

import hbase.HBaseCRUD;

import java.util.ArrayList;
import java.util.Date;

public class TimeCrawler {
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static int threadCount = 10;

	public static void qqUrlCrawler(Date date) {
		ArrayList<String> c = new ArrayList<String>();// cartoon
		ArrayList<String> t = new ArrayList<String>();// tv
		ArrayList<String> m = new ArrayList<String>();// movie
		ArrayList<String> z = new ArrayList<String>();// zongyi
		z = URLMakerZongYi.connect(z);
		try {
			c = URLMakercartoon.connect(c);
			m = URLMaker.connect(m);
			t = URLMakertv.connect(t);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < c.size(); i++) {
			m.add(c.get(i));
		}
		for (int i = 0; i < t.size(); i++) {
			m.add(t.get(i));
		}
		for (int i = 0; i < z.size(); i++) {
			m.add(z.get(i));
		}

		for (int i = 0; i < m.size(); i++) {
			System.out.println(m.get(i));
		}
		crawler(date, 1, m); //根据最基本的url得到每个视频的地址
	}

	public static void crawler(Date d, int step, ArrayList<String> urlList) {
		System.out.println(System.currentTimeMillis());
		ArrayList<CrawlerThread> pool = new ArrayList<CrawlerThread>();
		CrawlerThread.urlList = urlList;
		for (int i = 0; i < threadCount; i++) {
			CrawlerThread crawlerthread = new CrawlerThread(d.getTime(),
					d.toString(), step, pool);
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
				if (pool.size() == 0) {
					break;
				}
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
