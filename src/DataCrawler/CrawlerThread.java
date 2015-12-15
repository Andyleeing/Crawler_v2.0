package DataCrawler;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import Utils.JDBCConnection;
import Utils.SysParams;
import DataCrawler.DataCrawler_56.CrawlerContent;
import DataCrawler.Iqiyi.IqiyiCrawler;
import DataCrawler.SohuDataCrawler.SohuDataCrawler;
import DataCrawler.TencentDataCrawler.TencentDataCrawler;
import DataCrawler.XunleiDataCrawler.XunleiDataCrawler;
import DataCrawler.YoukuDataCrawler.YoukuDataCrawler;
import DataCrawler.leshiDataCrawler.LeshiDataCrawler;

public class CrawlerThread implements Runnable{

	public long time;
	public String date;
	public static ArrayList<String> urlList;
	public static FileWriter fw;
	public ArrayList<CrawlerThread> pool;
	YoukuDataCrawler youkuCrawler;
	XunleiDataCrawler xunleiCrawler;
	IqiyiCrawler iqiyiCrawler;
	LeshiDataCrawler lsCrawler;
	TencentDataCrawler tencentCrawler;
	SohuDataCrawler sohuCrawler;
	CrawlerContent _56crawler;
	public JDBCConnection jdbc=new JDBCConnection();
	ArrayList<String> failList = new ArrayList<String>();
	int tag = 0;
	public CrawlerThread(long time, String date,ArrayList<CrawlerThread> pool) {
		this.time = time;
		this.date = date;
		this.pool = pool;
		youkuCrawler = new YoukuDataCrawler(time,date,jdbc);
		xunleiCrawler = new XunleiDataCrawler(time,jdbc);
		iqiyiCrawler = new IqiyiCrawler(time,date,jdbc);
		lsCrawler = new LeshiDataCrawler(time,jdbc);
		tencentCrawler = new TencentDataCrawler(jdbc);
		_56crawler = new CrawlerContent(jdbc);
		sohuCrawler = new SohuDataCrawler(jdbc,time);
	}

	public static Integer count = 0;
	@Override
	public void run() {
		while (true) {
			String url = "";
			synchronized (urlList) {
				if (urlList != null && urlList.size() > 0) {
					url = urlList.get(0);
					urlList.remove(0);
				} else if (urlList == null || urlList.size() == 0) {
			/*		if(urlList != null && urlList.size() == 0 && tag == 0) {
						urlList.addAll(failList);
						failList.clear();
						tag = 1;
						continue;
					}
					if(urlList != null && urlList.size() == 0 && tag == 1) {
						JDBCConnection conn1 = new JDBCConnection();
						String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('"+ SysParams.urlTable_Hbase_local + "','1',now(),'抓取数据失败URL个数：" + failList.size() + "','all','韩江雪')";
						conn1.update(sql1);
						conn1.closeConn();
						conn1 = null;
						try {
							FileWriter fw = new FileWriter("src/DataCrawler/FailedURLs.txt",true);
							for(int i = 0 ;i < failList.size();i++) {
								fw.append(date + " : " +failList.get(i) + "\n");
							}
							fw.flush();
							fw.close();
						} catch (IOException e) {
							// TODO 自动生成的 catch 块
							e.printStackTrace();
						}
					} */
					synchronized(pool) {
						pool.remove(this);
						System.out.println("线程数目 "+pool.size());
						break;
					}
				}
			}
			String splits[] = url.split(" ");
			if(splits.length >= 2) {
				int flag = 1;
				System.out.println("num " + count++ + "  url : " + url);
				System.out.println("线程数目 "+pool.size());
			
				if(splits[0].equals("youku")) {
					flag = youkuCrawler.crawler(url);
				} else if(splits[0].equals("xunlei")) {
					flag = xunleiCrawler.crawler(url);
				} else if(splits[0].equals("tencent")) {
					flag = tencentCrawler.crawler(url, time);
				} else if(splits[0].equals("leshi")) {
					flag = lsCrawler.crawler(url);
				} else if(splits[0].equals("56")) {
					flag = _56crawler.main56Content(url, time);
				} else if(splits[0].equals("Iqiyi")) {
					flag = iqiyiCrawler.crawler(url);
				}
	              else if(splits[0].equals("sohu")) {
	            flag = sohuCrawler.crawler(url,time);
				} 
				if(flag == -1) {
					failList.add(url);
					jdbc.log("", "", 1, splits[0], url, "URL抓取失败", 4);
//					String sql1 = "insert into LogTemp(machine,level,time,website,manager,content) values ('"+ SysParams.urlTable_Hbase_local + "','1',curdate(),'" +splits[0]+"','all','"+url+"')";
//					jdbc.update(sql1);
//					conn1.closeConn();
				}
					
			}
		}
		try {
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void saveData(String line1,String line2) {
		synchronized(fw) {
			try {
				fw.write(line1 + "\n");
				fw.write(line2 + "\n");
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
		}
	}
}
