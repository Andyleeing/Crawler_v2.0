package URLcrawler;

import hbase.HBaseCRUD;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import URLcrawler.Iqiyi.iqiyi;
import URLcrawler.Leshi.leshiUrlCrawler;
import URLcrawler.Sohu.SohuCrawler;
import URLcrawler.Tencent.TimeCrawler;
import URLcrawler.URLcrawler_56.CrawlerUrl;
import URLcrawler.Xunlei.CrawlerURLs;
import URLcrawler.Youku.YoukuURLCrawler;
import Utils.JDBCConnection;
import Utils.SysParams;

public class URLmain {

	public static void main(String[] args) {
		// TODO 自动生成的方法存根
		int todayDone = 0;
		while (true) {
			Date d = new Date();
			int hour = d.getHours();
			if (hour >= 15 && todayDone == 0) {
				todayDone = 1;
				JDBCConnection conn = new JDBCConnection();
				int level = 1;
				String content = "开始抓取URL";
				String sql = "insert into Log(machine,level,time,content,website,manager) values('192.168.0.118+crawler2.0-12', "
						+ level + "," + "now()" + ",'" + content + "','All web','ALL')";
				conn.update(sql);
				conn.closeConn();
				
				CrawlerURL();
				
				JDBCConnection conne = new JDBCConnection();

				content = "抓取URL结束";
				sql = "insert into Log(machine,level,time,content,website,manager) values('192.168.0.118+crawler2.0-12', "
						+ level + "," + "now()" + ",'" + content + "','All web','ALL')";
				conne.update(sql);
				conne.closeConn();
				dispatchURLs();// new change;
				System.out.println("Dispatch url end");//new change;

			} else if (hour < 15 && todayDone == 1) {
				todayDone = 0;
		//		dispatchURLs();
		//		System.out.println("Dispatch url end");
			}
			try {
				Thread.sleep(60000 * 15);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void createTable(String tablename, JDBCConnection conn) {

		String sql = "create table "
				+ tablename
				+ "( id int(11) primary key auto_increment, rowkey varchar(300) NOT NULL,   website varchar(10) NOT NULL,url varchar(200) NOT NULL)";
		System.out.println(sql);
		String exist = "SHOW TABLES LIKE '" + tablename + "'";
		if (conn.tableExist(exist) != null) {
			return;
		}
		try {
			System.out.println(conn.update(sql));
		} catch (Exception e) {

		}
	}

	/**
	 * 采集的URL存储到mysql中每日分表中以及HBase每个网站单独的总表中
	 */
	public static void CrawlerURL() {

		FileWriter fw = null;
		try {
			fw = new FileWriter("src/URLcrawler/Log.txt", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// //创建3天后mysql中URL总表
		Date date = new Date();
		SimpleDateFormat form = new SimpleDateFormat("yyyyMMdd");
		String df = form.format(date);
		long timeNow = date.getTime() / 1000;
		long timestamp = 1422115200;
		if (timeNow + 86400 * 3 >= timestamp) {
			timestamp = timeNow + 86400 * 3;
			JDBCConnection conn = new JDBCConnection();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String sd = sdf.format(new Date(Long.parseLong(timestamp + "000")));
			createTable("urls" + sd, conn);
			conn.closeConn();
		}
		
	        //shou
			 try {
					fw.write("sohu URL crawler begin at :" + new Date().toString() + "\n");
					fw.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			     SohuCrawler.sohuUrlCrawler(date);
				try {
					fw.write("sohu URL crawler end at :" + new Date().toString() + "\n");
					fw.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
		// /youkuURL
		 try {
		 fw.write("Youku URL crawler begin at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		 YoukuURLCrawler youkuURL = new YoukuURLCrawler();
		 youkuURL.crawler();
		 try {
		 fw.write("Youku URL crawler end at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		 
		 //iqiyiURL
		 try {
		 fw.write("iqiyi URL crawler begin at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		 iqiyi iqiyi = new iqiyi();
		 iqiyi.Iqiyi();
		 try {
		 fw.write("iqiyi URL crawler end at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		
			
		// /tencentURL
		 try {
		 fw.write("tencent URL crawler begin at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		 Date d = new Date();
		 TimeCrawler.qqUrlCrawler(d);
		 try {
		 fw.write("tencent URL crawler end at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 } 
		//xunleiURL
		 try {
		 fw.write("xunlei URL crawler begin at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		 CrawlerURLs crawlerURls = new CrawlerURLs();
		 crawlerURls.crawler();
		 try {
		 fw.write("xunlei URL crawler end at :" + new Date().toString() +
		 "\n");
		 fw.flush();
		 } catch (IOException e) {
		 e.printStackTrace();
		 }
		// /lsURL
	try {
			fw.write("ls URL crawler begin at :" + new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		leshiUrlCrawler.LeshiUrl();
		try {
			fw.write("ls URL crawler end at :" + new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 56URL
		try {
			fw.write("56 URL crawler begin at :" + new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		CrawlerUrl.main56();
		try {
			fw.write("56 URL crawler end at :" + new Date().toString() + "\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	
	}

	public static void LoadParams() {
		JDBCConnection conn = new JDBCConnection();
		String sql1 = "select VMcount from CrawlerParams";
		ResultSet rs = conn.executeQuerySingle(sql1);
		try {
			if (rs.next()) {
				SysParams.VMcount = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.closeConn();
		conn = null;
	}

	public static void dispatchURLs() {
		LoadParams();
		int count = 0;
		HBaseCRUD hbase = new HBaseCRUD();
		String[] tableNames = new String[SysParams.VMcount];
		String[] st = { "C" };
		long rowkey = 1;
		for (int i = 0; i < tableNames.length; i++) {
			String tableName = "crawler-" + i;
			try {
				hbase.dropTable(tableName);
				hbase.createHTable(tableName, st);
			} catch (MasterNotRunningException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			tableNames[i] = tableName;
		}

		ResultScanner ykrs = null;
		ResultScanner iyrs = null;
		ResultScanner lsrs = null;
		ResultScanner xlrs = null;
		ResultScanner txrs = null;
		ResultScanner _56rs = null;
		ResultScanner shrs = null;
		try {
			ykrs = hbase.queryAll(SysParams.urlTable_Hbase_yk);
			iyrs = hbase.queryAll(SysParams.urlTable_Hbase_iy);
			lsrs = hbase.queryAll(SysParams.urlTable_Hbase_ls);
			xlrs = hbase.queryAll(SysParams.urlTable_Hbase_xl);
			txrs = hbase.queryAll(SysParams.urlTable_Hbase_tx);
			_56rs = hbase.queryAll(SysParams.urlTable_Hbase_56);
			shrs = hbase.queryAll(SysParams.urlTable_Hbase_sh);
			Iterator<Result> iteyk = ykrs.iterator();
			Iterator<Result> iteiy = iyrs.iterator();
			Iterator<Result> itels = lsrs.iterator();
			Iterator<Result> itexl = xlrs.iterator();
			Iterator<Result> itetx = txrs.iterator();
			Iterator<Result> ite56 = _56rs.iterator();
			Iterator<Result> itesh = shrs.iterator();
			ArrayList<ResultScanner> rss = new ArrayList<ResultScanner>();
			ArrayList<Iterator<Result>> ites = new ArrayList<Iterator<Result>>();
			rss.add(ykrs);
			rss.add(iyrs);
			rss.add(lsrs);
			rss.add(xlrs);
			rss.add(txrs);
			rss.add(_56rs);
			rss.add(shrs);
			ites.add(iteyk);
			ites.add(iteiy);
			ites.add(itels);
			ites.add(itexl);
			ites.add(itetx);
			ites.add(ite56);
			ites.add(itesh);
			int indexTable = 0;
			int indexRs = 0;
			ArrayList<String> all = new ArrayList<String>();
			while (rss.size() > 0) {
				Iterator<Result> ite = ites.get(indexRs);
				if (ite.hasNext()) {
					Result r = ite.next();
					String keyString = "";
					byte[] key = r.getRow();
					if (key == null)
						continue;
					keyString = new String(key, "utf-8");
					String urlString = "";
					byte[] url = r.getValue("C".getBytes(), "url".getBytes());
					if (url == null)
						continue;
					urlString = new String(url, "utf-8");
					
					all.add(urlString);
					count++;
					indexRs++;
					if(urlString.startsWith("youku ")) {
						if (ite.hasNext()) {
							 r = ite.next();
						    key = r.getRow();
							if (key == null)
								continue;
							keyString = new String(key, "utf-8");
						    url = r.getValue("C".getBytes(), "url".getBytes());
							if (url == null)
								continue;
							urlString = new String(url, "utf-8");							
							all.add(urlString);
						}  else {
							ResultScanner rs = rss.get(indexRs);
							if (rs != null) {
								rs.close();
								rs = null;
							}
							rss.remove(indexRs);
							ites.remove(indexRs);
							if (indexRs >= rss.size())
								indexRs = rss.size() - 1; 
						}
					}

					if (indexRs >= rss.size()) {

						for (int i = 0; i < rss.size()+1; i++) {
							String urll = all.get(i);
							String[] rows = null;
							String[] colfams = null;
							String[] quals = null;
							String[] values = null;
							rows = new String[] { new Date().getTime()+"" };
						//	rowkey++;
							 Thread.sleep(1);
							colfams = new String[] { "C" };
							quals = new String[] { "url" };
							values = new String[] { urll };
							try {
								hbase.putRows(tableNames[indexTable], rows,
										colfams, quals, values);
							} catch (Exception e) {
								e.printStackTrace();
							}
							rows = null;
							colfams = null;
							quals = null;
							values = null;
						}
						 indexTable++;
						 if (indexTable == SysParams.VMcount)
						 indexTable = 0;
						all.clear();
						indexRs = 0;
					}

				} else {
					ResultScanner rs = rss.get(indexRs);
					if (rs != null) {
						rs.close();
						rs = null;
					}
					rss.remove(indexRs);
					ites.remove(indexRs);
					if (indexRs >= rss.size())
						indexRs = rss.size() - 1; 
				}

			
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (ykrs != null) {
				ykrs.close();
				ykrs = null;
			}
			if (iyrs != null) {
				iyrs.close();
				iyrs = null;
			}
			if (lsrs != null) {
				lsrs.close();
				lsrs = null;
			}
			if (xlrs != null) {
				xlrs.close();
				xlrs = null;
			}
			if (txrs != null) {
				txrs.close();
				txrs = null;
			}
			if (_56rs != null) {
				_56rs.close();
				_56rs = null;
			}
			if (shrs != null) {
				shrs.close();
				shrs = null;
			}
		}
		try {
			hbase.commitPuts();
		} catch (Exception e) {
			e.printStackTrace();
		}
		hbase = null;
		System.out.println(count);
	}
}
