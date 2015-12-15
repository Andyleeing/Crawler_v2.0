package DataCrawler;

import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import Utils.SysParams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Datamain {

	public static void LoadParams() {
		JDBCConnection conn = new JDBCConnection();
		String sql1 = "select * from CrawlerParams where machine = '"
				+ SysParams.urlTable_Hbase_local + "'";
		ResultSet rs = conn.executeQuerySingle(sql1);
		try {
			if (rs.next()) {
				SysParams.timesperDay = rs.getInt(3);
				SysParams.DataCrawlerThreadCount = rs.getInt(4);
				SysParams.URLCrawlerThreadCount = rs.getInt(5);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.closeConn();
		conn = null;
	}

	public static void main(String[] args) {
	//	saveToHdfs("/usr/data/");
		while (true) {
			Date start = new Date();
			if (start.getHours() == 1 ||start.getHours() == 13) {
			downloadUrls();
			LoadParams();
		 long startTime = 0;

				Calendar c = Calendar.getInstance();// 可以对每个时间域单独修改

				int year = c.get(Calendar.YEAR);
				int month = c.get(Calendar.MONTH);
				int date = c.get(Calendar.DATE);
				int hour = c.get(Calendar.HOUR_OF_DAY);
				c.set(year, month, date, hour, 0, 0);
				startTime = start.getTime();
				crawler(c.getTime());
      
/*				long endTime = new Date().getTime();
				long interval = endTime - startTime;
				long intervalMis = 24 / SysParams.timesperDay * 60 * 60 * 1000;
				if (interval <= intervalMis) {
					try {
						Thread.sleep(intervalMis - interval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					try {
						Thread.sleep(2 * intervalMis - interval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} */
			
			} else {
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}

	public static void downloadUrls() {
		FileWriter fw = null;
		try {
			fw = new FileWriter(SysParams.locaLogfilePath, true);
			fw.write("downloadURLs begin at:" + new Date().toString() + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		HBaseCRUD hbase = new HBaseCRUD();
		BufferedWriter bw = null;
		ResultScanner rs = null;
		try {
			bw = new BufferedWriter(new FileWriter(SysParams.urlfilePath));
			rs = hbase.queryAll("urlshnew");
			Iterator<org.apache.hadoop.hbase.client.Result> ite = rs.iterator();
			while (ite.hasNext()) {
				String urlString = "";
				org.apache.hadoop.hbase.client.Result r = ite.next();
				byte[] url = r.getValue("C".getBytes(), "url".getBytes());
				if (url == null)
					continue;
				urlString = new String(url, "utf-8");
				bw.write(urlString + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (rs != null)
					rs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			fw.write("downloadURLs end at:" + new Date().toString() + "\n");
			fw.flush();
			fw.close();
			fw = null;
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void crawler(Date date) {
		CrawlerThread.count = 0;
		FileWriter fw = null;
		try {
			fw = new FileWriter(SysParams.locaLogfilePath, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		JDBCConnection conn1 = new JDBCConnection();
		String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('"
				+ SysParams.urlTable_Hbase_local
				+ "','1',now(),'抓取数据开始','yk','韩江雪')";
		conn1.update(sql1);
		conn1.closeConn();
		conn1 = null;
		try {
			fw.write("crawler begin at:" + new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File file = new File("/usr/data/" + SysParams.urlTable_Hbase_local
				+ date.getTime());
		FileWriter crawlfw = null; // 用于写抓取的大文件的写入流，传递给线程类，每个线程写入文件要加锁
		try {
			crawlfw = new FileWriter(file, true);
		} catch (IOException e2) {
			e2.printStackTrace();
			return;
		}
		ArrayList<CrawlerThread> pool = new ArrayList<CrawlerThread>();
		ArrayList<String> urlList = readURLs();
		CrawlerThread.urlList = urlList;
		CrawlerThread.fw = crawlfw;
		for (int i = 0; i < SysParams.DataCrawlerThreadCount; i++) {
			CrawlerThread crawlerthread = new CrawlerThread(date.getTime(),
					date.toString(), pool);
			pool.add(crawlerthread);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(crawlerthread).start();
			crawlerthread = null;
		}
		while (true) {
			try {
				Thread.sleep(6000*5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			synchronized (pool) {
				if (pool.size() <= 5) {
					System.out.println("a loop is done");
					break;
				}
			}
		}
		pool = null;
		if (crawlfw != null) {
			try {
				crawlfw.flush();
				crawlfw.close();
				crawlfw = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		saveToHdfs("/usr/data/" + SysParams.urlTable_Hbase_local
				+ date.getTime());
		JDBCConnection conn11 = new JDBCConnection();
		String sql11 = "insert into Log(machine,level,time,content,website,manager) values('"
				+ SysParams.urlTable_Hbase_local
				+ "','1',now(),'抓取数据结束','yk','韩江雪')";
		conn11.update(sql11);
		conn11.closeConn();
		conn11 = null;
		try {
			fw.write("crawler end at:" + new Date().toString() + "\n");
			fw.flush();
			fw.close();
			fw = null;
		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	public static ArrayList<String> readURLs() {
		ArrayList<String> urls = new ArrayList<String>();
		try {
			BufferedReader bw = new BufferedReader(new FileReader(
					SysParams.urlfilePath));
			String url = null;
			while ((url = bw.readLine()) != null) {
				if (url != null && !url.equals("")) {
					if (url.substring(0, 2).equals("56"))
						urls.add(url + "@" + 1);// 56
					else
						urls.add(url);
				}
				// urls.add(url);
			}
			bw.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return urls;
	}

	public static void saveToHdfs(String parsefile) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(SysParams.locaLogfilePath, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Configuration conf = new Configuration();
		String uri = "hdfs://192.168.10.10:9000";
		conf.set("fs.default.name", "hdfs://192.168.10.10:9000");
		conf.addResource(new Path("src/hdfs-site.xml"));
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(URI.create(uri), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Path source = new Path(parsefile);
		int index = parsefile.lastIndexOf("/");
		String name = "/crawldatas/";
		if (index > -1)
			name += parsefile.substring(parsefile.lastIndexOf("/") + 1);
		else
			name += System.currentTimeMillis() + "";
		Path destination = new Path(name);
		try {
			fw.write("write file" + destination.getName() + " to hdfs begin:"
					+ new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			hdfs.copyFromLocalFile(source, destination);
			File file = new File(parsefile);
			file.delete();
			String rowKey = name;
			String[] inforows = { rowKey };
			String[] infocolfams = { "C" };
			String[] infoquals = { "dir" };
			String[] infovalues = { name };
			HBaseCRUD hbase = new HBaseCRUD();
			try {
				hbase.putRows(SysParams.parseTable_Hbase, inforows,
						infocolfams, infoquals, infovalues);
				hbase.commitPuts();
			} catch (Exception e) {
				e.printStackTrace();
			}
			inforows = null;
			infocolfams = null;
			infoquals = null;
			infovalues = null;
		} catch (IOException e) {
			try {
				JDBCConnection conn1 = new JDBCConnection();
				String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('DataCrawler','2',now(),'上传HDFS出错','yk','韩江雪')";
				conn1.update(sql1);
				conn1.closeConn();
				conn1 = null;
				fw.write("write to hdfs error:" + new Date().toString() + "\n");
				fw.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		File file = new File(parsefile);
		file.delete();
		try {
			fw.write("write to hdfs end:" + new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
