package ParserData.YoukuParserData;



import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import Utils.TextValue;


public class ImportInfoVideo_v2 {
	public static FileWriter fw = null;
	public static void main(String[] args) {
		long timestamp = 1422115200;
		int threadCount = 3;
		try {
			fw = new FileWriter("refrence.txt");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//referenceToMysql();
		while (true) {
			////for dynamic
			/////create new tables////////////
			Date now = new Date();
			if(now.getHours() > 21 || now.getHours() < 17) {
				try {
					Thread.sleep(720000 * 2);
				}catch(Exception e) {
				}
				continue;
			}
			long timeNow = now.getTime() / 1000;
			if(timeNow + 86400 * 3 >= timestamp) {
				timestamp = timeNow + 86400 * 3;
				JDBCConnection conn = new JDBCConnection();
				SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
				String sd = sdf.format(new Date(Long.parseLong(timestamp +"000")));
				ExportEveryDay.createvideoTable("videodynamic" + sd, conn);
				conn.closeConn();
			}
			//////////////////////////////////////////////////////////
			System.out.println("video begin");
			
			String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出video数据开始','yk','韩江雪')";
			JDBCConnection conn1 = new JDBCConnection();
			conn1.update(sql1);
			conn1.closeConn();
			conn1 = null;
			
			HBaseCRUD crud = new HBaseCRUD();
			ResultScanner rs = null;
			JDBCConnection jdbconn = new JDBCConnection();
			try {
				rs = crud.queryAll("videoinfo2");
			} catch (IOException e) {
			}
			Iterator<Result> ite = rs.iterator();
			try {
				while (ite.hasNext()) {
					String keyString = "";
					String showtypeString = "";
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
					if(websiteString == null || !websiteString.equals("yk"))
						continue;
					byte[] showtype = r.getValue("B".getBytes(),
							"showtype".getBytes());
					if (showtype != null) {
						showtypeString = new String(showtype, "utf-8");
					}
					byte[] done = r.getValue("C".getBytes(), "doneyk1".getBytes());
					if (done != null) {
						String doneString = new String(done, "utf-8");
						if (doneString.equals("1"))
							continue;
					}
		////////////这里要把showtype做为参数传递给各自的函数//////////////////////////
		///////////判断各自的showtype，写入mysql时统一：4种类型：正片   预告片  MV 花絮
					if(!(youkuInfoImportvideo(showtypeString,keyString,r,jdbconn) == -1)) {
							String[] inforows = { keyString };
							String[] infocolfams = { "C" };
							String[] infoquals = { "doneyk1" };
							String[] infovalues = { "1" };
							try {
								crud.putRows("videoinfo2", inforows, infocolfams,
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
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				jdbconn.closeConn();
			}catch(Exception e) 
			{
				e.printStackTrace();
			}
			rs.close();
			try {
				crud.commitPuts();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			System.out.println("videoinfo end");
			/////////dynamic export/////////////
			System.out.println("dynamic begin");
			ResultScanner rs1 = null;
			try {
				rs1 = crud.queryAll("videodynamicbak2");
			} catch (IOException e) {
			}
			Iterator<Result> ite1 = rs1.iterator();
			ArrayList<ImportThread1Video_v2> pool = new ArrayList<ImportThread1Video_v2>(); // 抓取线程池
			ImportThread1Video_v2.ite1= ite1;
			for (int i = 0; i < threadCount; i++) {
				ImportThread1Video_v2 crawlerthread = new ImportThread1Video_v2();
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
			while(true) {
				if(pool.size() == 0) {
					ImportThread1Video_v2.crud1.delete_Video();
					///////////deal with recommend/////////////////////
					referenceDone();
					//referenceToMysql();
					break;
				}
				try {
					Thread.sleep(60000);
				}catch(Exception e) {
				}
			}
			System.out.println("dynamic end");
			
			String sql2 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出video数据结束','yk','韩江雪')";
			JDBCConnection conn2 = new JDBCConnection();
			conn2.update(sql2);
			conn2.closeConn();
			conn2 = null;
			try {
				Thread.sleep(720000 * 5);
			}catch(Exception e) {
			}
			if(rs1 != null)
				rs.close();
			//////////////////////////////////////////////////////////
		}
	}
	
	public static void export() {
		long timestamp = 1422115200;
		int threadCount = 10;
			Date now = new Date();
			long timeNow = now.getTime() / 1000;
			if(timeNow + 86400 * 3 >= timestamp) {
				timestamp = timeNow + 86400 * 3;
				JDBCConnection conn = new JDBCConnection();
				SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
				String sd = sdf.format(new Date(Long.parseLong(timestamp +"000")));
				ExportEveryDay.createvideoTable("videodynamic" + sd, conn);
				conn.closeConn();
			}
			//////////////////////////////////////////////////////////
			System.out.println("video begin");
			
			String sql1 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出video数据开始','yk','韩江雪')";
			JDBCConnection conn1 = new JDBCConnection();
			conn1.update(sql1);
			conn1.closeConn();
			conn1 = null;
			
			HBaseCRUD crud = new HBaseCRUD();
			ResultScanner rs = null;
			JDBCConnection jdbconn = new JDBCConnection();
			try {
				rs = crud.queryAll("videoinfo2");
			} catch (IOException e) {
			}
			Iterator<Result> ite = rs.iterator();
			try {
				while (ite.hasNext()) {
					String keyString = "";
					String showtypeString = "";
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
					if(websiteString == null || !websiteString.equals("yk"))
						continue;
					byte[] showtype = r.getValue("B".getBytes(),
							"showtype".getBytes());
					if (showtype != null) {
						showtypeString = new String(showtype, "utf-8");
					}
					byte[] done = r.getValue("C".getBytes(), "doneyk1".getBytes());
					if (done != null) {
						String doneString = new String(done, "utf-8");
						if (doneString.equals("1"))
							continue;
					}
		////////////这里要把showtype做为参数传递给各自的函数//////////////////////////
		///////////判断各自的showtype，写入mysql时统一：4种类型：正片   预告片  MV 花絮
					if(!(youkuInfoImportvideo(showtypeString,keyString,r,jdbconn) == -1)) {
							String[] inforows = { keyString };
							String[] infocolfams = { "C" };
							String[] infoquals = { "doneyk1" };
							String[] infovalues = { "1" };
							try {
								crud.putRows("videoinfo2", inforows, infocolfams,
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
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				jdbconn.closeConn();
			}catch(Exception e) 
			{
				e.printStackTrace();
			}
			rs.close();
			try {
				crud.commitPuts();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			System.out.println("videoinfo end");
			/////////dynamic export/////////////
			System.out.println("dynamic begin");
			ResultScanner rs1 = null;
			try {
				rs1 = crud.queryAll("videodynamicbak2");
			} catch (IOException e) {
			}
			Iterator<Result> ite1 = rs1.iterator();
			ArrayList<ImportThread1Video_v2> pool = new ArrayList<ImportThread1Video_v2>(); // 抓取线程池
			ImportThread1Video_v2.ite1= ite1;
			for (int i = 0; i < threadCount; i++) {
				ImportThread1Video_v2 crawlerthread = new ImportThread1Video_v2();
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
			while(true) {
				if(pool.size() == 0) {
					ImportThread1Video_v2.crud1.delete_Video();
					///////////deal with recommend/////////////////////
					referenceDone();
					break;
				}
				try {
					Thread.sleep(60000);
				}catch(Exception e) {
				}
			}
			System.out.println("dynamic end");
			
			String sql2 = "insert into Log(machine,level,time,content,website,manager) values ('192.168.0.122+Crawl-7',1,now(),'导出video数据结束','yk','韩江雪')";
			JDBCConnection conn2 = new JDBCConnection();
			conn2.update(sql2);
			conn2.closeConn();
			conn2 = null;
			
			if(rs1 != null)
				rs.close();
			//////////////////////////////////////////////////////////
	}
	public static void referenceDone() {
		JDBCConnection conn = new JDBCConnection();
		for(int i = 0;i < ImportThread1Video_v2.reflist.size();i++) {
			TextValue tv = (TextValue)ImportThread1Video_v2.reflist.get(i);
			String[] splits = tv.text.split("@@@");
			if(splits != null && splits.length == 2) {
				String rowkey = splits[0];
				String tableName = splits[1];
				if(tableName.indexOf("videodynamic") >= 0){
					continue;
				}
				String date = tableName.substring(tableName.indexOf("2"));
				String count = tv.value.toString();
				String sql = "insert into reference" + date +" (rowkey,reference,website) values ('" + rowkey +"','" + count +"','yk')";
				conn.update(sql);
			}
		}
		conn.closeConn();
		conn = null;
		ImportThread1Video_v2.reflist.clear();
	}
	public static String  cleanString(String str){
		str=str.replaceAll(" ", "");
		str=str.replaceAll("'", "");
		str=str.replaceAll("-", "");
		str=str.replaceAll("\n", "");
		str=str.replaceAll("\r", "");
		str=str.replaceAll("//s", "");
		return str;
	}
	public static int ConvertToInt(String str) {
		int value = -1;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return value;
	}
	public static void insertVideoInfo(String keyString,Result r,JDBCConnection jdbconn) {
		try{		
				String inforowkeyString = "";
				String playrowkeyString = "";
				String commentString = "";
				String showtypeString = "";
				byte[] inforowkey = r.getValue("R".getBytes(),
						"inforowkey".getBytes());
				if (inforowkey == null)	return ;
				inforowkeyString = new String(inforowkey, "utf-8");
				inforowkeyString=cleanString(inforowkeyString);
				byte[] playrowkey = r.getValue("R".getBytes(),
						"playrowkey".getBytes());
				if (playrowkey == null)	return;
				playrowkeyString = new String(playrowkey, "utf-8");
				playrowkeyString=cleanString(playrowkeyString);
				byte[] url = r.getValue("B".getBytes(),
						"url".getBytes());
				if (url == null)	return;
				String urlString = new String(url, "utf-8");
				urlString=cleanString(urlString);
				byte[] name = r.getValue("B".getBytes(),
						"name".getBytes());
				if (name == null)	return;
				String nameString = new String(name, "utf-8");
				nameString=cleanString(nameString);
				ArrayList<TextValue> values = new ArrayList<TextValue>();
				byte[] comment = r.getValue("C".getBytes(),"comment".getBytes());
				if (comment != null){
				commentString = new String(comment, "utf-8");
				commentString=cleanString(commentString);
				int commentInt=ConvertToInt(commentString);
				TextValue commenttv = new TextValue();
				commenttv.text = "comment";
				commenttv.value = commentInt;
				values.add(commenttv);
				}
				byte[] showtype = r.getValue("B".getBytes(),
						"showtype".getBytes());
				if (showtype != null){
				showtypeString = new String(showtype, "utf-8");
				showtypeString=cleanString(showtypeString);
				TextValue showtypetv = new TextValue();
				showtypetv.text = "showtype";
				showtypetv.value = showtypeString;
				values.add(showtypetv);
				}else {
					TextValue showtypetv = new TextValue();
					showtypetv.text = "showtype";
					showtypetv.value = "正片";
					values.add(showtypetv);
				}
				TextValue rowkeytv = new TextValue();
				rowkeytv.text = "rowkey";
				rowkeytv.value = inforowkeyString;
				values.add(rowkeytv);
				TextValue inforowkeytv = new TextValue();
				inforowkeytv.text = "inforowkey";
				inforowkeytv.value = inforowkeyString;
				values.add(inforowkeytv);
				TextValue playrowkeytv = new TextValue();
				playrowkeytv.text = "playrowkey";
				playrowkeytv.value = playrowkeyString;
				values.add(playrowkeytv);
				TextValue urltv = new TextValue();
				urltv.text = "url";
				urltv.value = urlString;
				values.add(urltv);
				TextValue nametv = new TextValue();
				nametv.text = "name";
				nametv.value = nameString;
				values.add(nametv);
				TextValue namewebsite = new TextValue();
				namewebsite.text = "website";
				namewebsite.value = "sh";
				values.add(namewebsite);
				jdbconn.insert(values, "videoinfo");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static int youkuInfoImportvideo(String showtype,String keyString,Result r1,JDBCConnection jdbconn) {
		
		byte[] inforowkey = r1.getValue("R".getBytes(),
				"inforowkey".getBytes());
		if (inforowkey == null)
			return 1;
		String inforowkeyString = new String(inforowkey);
		if(inforowkeyString.equals(""))
			return 1;
		byte[] playrowkey = r1.getValue("R".getBytes(),
				"playrowkey".getBytes());
		if (playrowkey == null)
			return 1;
		String playrowkeyString = new String(playrowkey);
		if(inforowkeyString.equals(""))
			return 1;
		String nameString = "";
		byte[] name = r1.getValue("B".getBytes(),
				"name".getBytes());
		if (name == null)
			return -1;
		if (name != null)
			nameString = new String(name);
		if (nameString.equals(""))
			return -1;
		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = inforowkeyString +"+yk";
		values.add(rowkeytv);
		
		TextValue crawltimetv = new TextValue();
		crawltimetv.text = "crawltime";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = new Date();
		crawltimetv.value = sdf.format(d);
		values.add(crawltimetv);
		
		TextValue nametv = new TextValue();
		nametv.text = "name";
		nametv.value = nameString;
		values.add(nametv);
		
		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "yk";
		values.add(namewebsite);
		
		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = inforowkeyString;
		values.add(inforowkeytv);
		
		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = playrowkeyString;
		values.add(playrowkeytv);
		
		if(!showtype.equals("")) {
			TextValue showtypetv = new TextValue();
			showtypetv.text = "showtype";
			showtypetv.value = showtype;
			values.add(showtypetv);
		}
		
		if(nameString.equals("游艇汇 130602")) {
			System.out.println(nameString);
		}
		if(jdbconn.insert(values, "videoinfo") < 0)
			return -1;
		else 
			return 1;
	}
}
