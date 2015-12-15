package URLcrawler.Tencent;

import hbase.HBaseCRUD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;

public class CrawlerThread implements Runnable {
	public long time;
	public String date;
	public static ArrayList<String> urlList;
	public static HashSet<String> infoList = new HashSet<String>();
	static Integer k = 2;
	public HashSet<String> alreallyinList = new HashSet<String>();
	public int i = 100;
	public static ArrayList<CrawlerThread> pool;
	HBaseCRUD hbase = new HBaseCRUD();
	int step;
	
	OutputFile opfm = new OutputFile(hbase);
	OutPutFilecarton opfc = new OutPutFilecarton(hbase);
	OutputFileTV opftv = new OutputFileTV(hbase);
	OutputFileZongYi opfz=new OutputFileZongYi(hbase);
	
	public CrawlerThread(long time, String date, int step,
			ArrayList<CrawlerThread> pool) {
		this.time = time;
		this.date = date;
		this.step = step;
		CrawlerThread.pool = pool;
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
						pool.remove(this);
						try {
							hbase.commitPuts();
						} catch (Exception e) {
							e.printStackTrace();
						}
						break;
					}
				}
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String sd = sdf.format(time);
			String tableName = "urls" + sd;

			if (url.indexOf("movie") > 0) {
				opfm.tencentOutput(url, tableName);
			} else if (url.indexOf("cart") > 0) {
				opfc.tencentOutput(url, tableName);
			} else if (url.contains("variety")) {
				opfz.tencentOutput(url, tableName);
			} else {
				opftv.tencentOutput(url, tableName);
			}

		}
	}
}
