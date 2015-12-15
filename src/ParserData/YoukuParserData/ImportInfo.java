package ParserData.YoukuParserData;

import hbase.HBaseCRUD;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import Utils.TextValue;

public class ImportInfo {

	public static void main(String[] args) {
		long timestamp = 1422115200;
		int threadCount = 1;
		while (true) {
			Date now = new Date();
			if (now.getHours() > 21 || now.getHours() < 17) {
				try {
					Thread.sleep(720000 * 2);
				} catch (Exception e) {
				}
				continue;
			}

			// //for dynamic
			ArrayList<String> keyStringList = new ArrayList<String>();
			ArrayList<String> weblist = new ArrayList<String>();
			ArrayList<String> categorylist = new ArrayList<String>();
			// ////////////////////

			// ///create new tables////////////
			long timeNow = now.getTime() / 1000;
			if (timeNow + 86400 * 3 >= timestamp) {
				timestamp = timeNow + 86400 * 3;
				JDBCConnection conn = new JDBCConnection();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				String sd = sdf.format(new Date(Long.parseLong(timestamp
						+ "000")));
				ExportEveryDay.createTable("moviedynamic" + sd, conn);
				conn.closeConn();
			}
			// ////////////////////////////////////////////////////////
			System.out.println("info begin");

			String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出movie数据开始','yk','韩江雪')";
			JDBCConnection conn1 = new JDBCConnection();
			conn1.update(sql1);
			conn1.closeConn();
			conn1 = null;

			HBaseCRUD crud = new HBaseCRUD();
			ResultScanner rs = null;
			JDBCConnection jdbconn = new JDBCConnection();
			try {
				rs = crud.queryAll("movieinfo2");
			} catch (IOException e) {
			}
			Iterator<Result> ite = rs.iterator();
			try {
				while (ite.hasNext()) {
					String keyString = "";
					String categoryString = "";
					String websiteString = "";
					Result r = ite.next();
					byte[] key = r.getRow();
					if (key == null) {
						continue;
					}
					keyString = new String(key, "utf-8");
					byte[] website = r.getValue("R".getBytes(),
							"website".getBytes());
					if (website == null)
						continue;

					websiteString = new String(website, "utf-8");

					if (!websiteString.equals("yk"))
						continue;
					byte[] category = r.getValue("B".getBytes(),
							"category".getBytes());
					if (category == null)
						continue;
					categoryString = new String(category, "utf-8");

					keyStringList.add(keyString);
					weblist.add(websiteString);
					categorylist.add(categoryString);

					byte[] done = r.getValue("C".getBytes(),
							"doneyk1".getBytes());
					if (done != null) {
						String doneString = new String(done, "utf-8");
						if (doneString.equals("1"))
							continue;
					}
					// //////////这里要把category做为参数传递给各自的函数//////////////////////////
					// /////////判断各自的category，写入mysql时统一：电影：movie 电视剧 tv 综艺
					if (!(youkuInfoImport(categoryString, keyString, r, jdbconn) == -1)) {
						String[] inforows = { keyString };
						String[] infocolfams = { "C" };
						String[] infoquals = { "doneyk1" };
						String[] infovalues = { "1" };
						try {
							crud.putRows("movieinfo2", inforows, infocolfams,
									infoquals, infovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
						inforows = null;
						infocolfams = null;
						infoquals = null;
						infovalues = null;
					}
				}
				rs.close();
				crud.commitPuts();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				jdbconn.closeConn();
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("info end");
			// ///////dynamic export/////////////
			System.out.println("dynamic begin");
			ArrayList<ImportThread1> pool = new ArrayList<ImportThread1>(); // 抓取线程池
			ImportThread1.keyStringList = keyStringList;
			ImportThread1.webList = weblist;
			ImportThread1.categoryList = categorylist;
			for (int i = 0; i < threadCount; i++) {
				ImportThread1 crawlerthread = new ImportThread1();
				pool.add(crawlerthread);
				crawlerthread.pool = pool;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				new Thread(crawlerthread).start();
				crawlerthread = null;
			}
			while (true) {
				if (pool.size() == 0) {
					ImportThread1.crud1.delete_Movie();
					break;
				}
				try {
					Thread.sleep(60000);
				} catch (Exception e) {
				}
			}
			System.out.println("dynamic end");

			String sql2 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出movie数据结束','yk','韩江雪')";
			JDBCConnection conn2 = new JDBCConnection();
			conn2.update(sql2);
			conn2.closeConn();
			conn2 = null;
			try {
				Thread.sleep(720000 * 5);
			} catch (Exception e) {
			}
			// ////////////////////////////////////////////////////////
		}
	}

	public static void export() {
		// //for dynamic
		ArrayList<String> keyStringList = new ArrayList<String>();
		ArrayList<String> weblist = new ArrayList<String>();
		ArrayList<String> categorylist = new ArrayList<String>();
		// ////////////////////
		int threadCount = 5;
		Date now = new Date();
		// ///create new tables////////////
		long timeNow = now.getTime() / 1000;
		long timestamp = 1422115200;
		if (timeNow + 86400 * 3 >= timestamp) {
			timestamp = timeNow + 86400 * 3;
			if (timeNow + 86400 * 3 >= timestamp) {
				timestamp = timeNow + 86400 * 3;
				JDBCConnection conn = new JDBCConnection();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				String sd = sdf.format(new Date(Long.parseLong(timestamp
						+ "000")));
				ExportEveryDay.createTable("moviedynamic" + sd, conn);
				conn.closeConn();
			}
			// ////////////////////////////////////////////////////////
			System.out.println("info begin");

			String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('DataParse',1,now(),'导出movie数据开始','yk','韩江雪')";
			JDBCConnection conn1 = new JDBCConnection();
			conn1.update(sql1);
			conn1.closeConn();
			conn1 = null;

			HBaseCRUD crud = new HBaseCRUD();
			ResultScanner rs = null;
			JDBCConnection jdbconn = new JDBCConnection();
			try {
				rs = crud.queryAll("movieinfo2");
			} catch (IOException e) {
			}
			Iterator<Result> ite = rs.iterator();
			try {
				while (ite.hasNext()) {
					String keyString = "";
					String categoryString = "";
					String websiteString = "";
					Result r = ite.next();
					byte[] key = r.getRow();
					if (key == null) {
						continue;
					}
					keyString = new String(key, "utf-8");
					byte[] website = r.getValue("R".getBytes(),
							"website".getBytes());
					if (website == null)
						continue;

					websiteString = new String(website, "utf-8");

					if (!websiteString.equals("yk"))
						continue;
					byte[] category = r.getValue("B".getBytes(),
							"category".getBytes());
					if (category == null)
						continue;
					categoryString = new String(category, "utf-8");

					keyStringList.add(keyString);
					weblist.add(websiteString);
					categorylist.add(categoryString);

					byte[] done = r.getValue("C".getBytes(),
							"doneyk1".getBytes());
					if (done != null) {
						String doneString = new String(done, "utf-8");
						if (doneString.equals("1"))
							continue;
					}
					// //////////这里要把category做为参数传递给各自的函数//////////////////////////
					// /////////判断各自的category，写入mysql时统一：电影：movie 电视剧 tv 综艺
					if (!(youkuInfoImport(categoryString, keyString, r, jdbconn) == -1)) {
						String[] inforows = { keyString };
						String[] infocolfams = { "C" };
						String[] infoquals = { "doneyk1" };
						String[] infovalues = { "1" };
						try {
							crud.putRows("movieinfo2", inforows, infocolfams,
									infoquals, infovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
						inforows = null;
						infocolfams = null;
						infoquals = null;
						infovalues = null;
					}
				}
				rs.close();
				crud.commitPuts();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				jdbconn.closeConn();
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("info end");
			// ///////dynamic export/////////////
			System.out.println("dynamic begin");
			ArrayList<ImportThread1> pool = new ArrayList<ImportThread1>(); // 抓取线程池
			ImportThread1.keyStringList = keyStringList;
			ImportThread1.webList = weblist;
			ImportThread1.categoryList = categorylist;
			for (int i = 0; i < threadCount; i++) {
				ImportThread1 crawlerthread = new ImportThread1();
				pool.add(crawlerthread);
				crawlerthread.pool = pool;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				new Thread(crawlerthread).start();
				crawlerthread = null;
			}
			while (true) {
				if (pool.size() == 0) {
					ImportThread1.crud1.delete_Movie();
					break;
				}
				try {
					Thread.sleep(60000);
				} catch (Exception e) {
				}
			}
			System.out.println("dynamic end");

			String sql2 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出movie数据结束','yk','韩江雪')";
			JDBCConnection conn2 = new JDBCConnection();
			conn2.update(sql2);
			conn2.closeConn();
			conn2 = null;
			
		}
		// ////////////////////////////////////////////////////////
	}

	// /
	public static int youkuInfoImport(String category, String keyString,
			Result r, JDBCConnection jdbconn) {

		byte[] name = r.getValue("B".getBytes(), "name".getBytes());
		if (name == null)
			return -1;
		String nameString = "";
		nameString = new String(name);
		if (nameString.equals(""))
			return -1;
		String yearString = "";
		int yearint = -1;
		byte[] year = r.getValue("R".getBytes(), "year".getBytes());
		if (year != null) {
			yearString = new String(year);
			try {
				yearint = Integer.parseInt(yearString);
			} catch (Exception e) {
			}
		}
		String timeString = "";
		int timeint = -1;
		byte[] time = r.getValue("B".getBytes(), "time".getBytes());
		if (time != null) {
			timeString = new String(time);
			try {
				timeint = Integer.parseInt(timeString.replace("-", "").replace(
						" ", ""));
			} catch (Exception e) {
			}
		}
		String duraString = "-1";
		byte[] dura = r.getValue("B".getBytes(), "duration".getBytes());
		if (dura != null) {
			duraString = new String(dura);
			if (duraString.indexOf("分钟") >= 0)
				duraString = duraString.substring(0, duraString.indexOf("分钟"));
			if (duraString.equals(""))
				duraString = "-1";
		}

		String ytimeString = "";
		int ytiemint = -1;
		byte[] ytime = r.getValue("B".getBytes(), "ytime".getBytes());
		if (ytime != null) {
			ytimeString = new String(ytime);
			try {
				ytiemint = Integer.parseInt(ytimeString.replace("-", "")
						.replace(" ", ""));
			} catch (Exception e) {
			}
		}

		String lanString = "";
		byte[] lan = r.getValue("B".getBytes(), "lan".getBytes());
		if (lan != null) {
			lanString = new String(lan);
			int index = lanString.indexOf("@");
			if (index >= 0)
				lanString = lanString.substring(0, index);
		}
		if (lanString.indexOf(" ") > 0)
			lanString = lanString.substring(0, lanString.indexOf(" "));
		String areaString = "";
		byte[] area = r.getValue("B".getBytes(), "area".getBytes());
		if (area != null) {
			areaString = new String(area);
			int index = areaString.indexOf("@");
			if (index >= 0)
				areaString = areaString.substring(0, index);
		}
		if (areaString.indexOf(" ") > 0)
			areaString = areaString.substring(0, areaString.indexOf(" "));
		String directorString = "";
		byte[] director = r.getValue("B".getBytes(), "director".getBytes());
		if (director != null) {
			directorString = new String(director);
			int index = directorString.indexOf("@");
			if (index >= 0)
				directorString = cleanString(directorString.substring(0, index));
		}
		if (directorString.indexOf(" ") > 0)
			directorString = directorString.substring(0,
					directorString.indexOf(" "));
		String mainactorString = "";
		String[] mainactorSplits = null;
		byte[] mainactor = r.getValue("B".getBytes(), "mainactor".getBytes());
		if (mainactor != null) {
			mainactorString = new String(mainactor);
			mainactorSplits = mainactorString.split("@");
		}

		byte[] type = r.getValue("B".getBytes(), "type".getBytes());
		String[] typeSplits = null;
		if (type != null) {
			String typeString = new String(type);
			typeSplits = typeString.split("@");
		}

		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue crawltimetv = new TextValue();
		crawltimetv.text = "crawltime";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = new Date();
		crawltimetv.value = sdf.format(d);
		values.add(crawltimetv);

		TextValue yeartv = new TextValue();
		yeartv.text = "year";
		yeartv.value = yearint;
		values.add(yeartv);

		TextValue timetv = new TextValue();
		timetv.text = "time";
		timetv.value = timeint;
		values.add(timetv);

		TextValue ytimetv = new TextValue();
		ytimetv.text = "ytime";
		ytimetv.value = ytiemint;
		values.add(ytimetv);

		if (!lanString.equals("")) {
			TextValue lantv = new TextValue();
			lantv.text = "lan";
			lantv.value = lanString;
			values.add(lantv);
		}
		if (!areaString.equals("")) {
			TextValue areatv = new TextValue();
			areatv.text = "area";
			areatv.value = areaString;
			values.add(areatv);
		}
		if (!directorString.equals("")) {
			TextValue directortv = new TextValue();
			directortv.text = "director1";
			directortv.value = directorString;
			values.add(directortv);
		}
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = keyString;
		values.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		nametv.value = nameString;
		values.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "yk";
		values.add(namewebsite);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		categorytv.value = category;
		values.add(categorytv);

		if (typeSplits != null) {
			for (int i = 0; i < typeSplits.length; i++) {
				TextValue typetv = new TextValue();
				typetv.text = "type" + (i + 1);
				if (typeSplits[i] == null || typeSplits[i].equals("")) {
					if (i == 0) {
						typetv.value = "其他";
						values.add(typetv);
						break;
					} else
						break;
				}
				typetv.value = typeSplits[i];
				values.add(typetv);
				if (i == 2)
					break;
			}
		}
		if (mainactorSplits != null) {
			for (int i = 0; i < mainactorSplits.length; i++) {
				if (mainactorSplits[i].equals(""))
					break;
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				actortv.value = cleanString(mainactorSplits[i]);
				values.add(actortv);
				if (i == 2)
					break;
			}
		}

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		duratv.value = duraString;
		values.add(duratv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = "-1";
		values.add(pricetv);
		if (jdbconn.insert(values, "movieinfo") < 0)
			return -1;
		else
			return 1;
	}

	public static String cleanString(String str) {
		str = str.replaceAll(" ", "");
		str = str.replaceAll("'", "");
		str = str.replaceAll("-", "");
		str = str.replaceAll("\n", "");
		str = str.replaceAll("\r", "");
		str = str.replaceAll("//s", "");
		return str;
	}
}
