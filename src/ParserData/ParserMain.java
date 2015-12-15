package ParserData;

import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import ParserData.YoukuParserData.ExportEveryDay;
import Utils.JDBCConnection;
import Utils.SysParams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import HDFS.HDFSCrudImpl;

public class ParserMain {

	private static String dirString;
	private static String webSiteString;
	private static String keyString;
	public static int threadCount = 1;
	public static FileWriter fw;
	public static Configuration conf = new Configuration();
	static String uri = "hdfs://192.168.10.10:9000";
	static HBaseCRUD hbase = new HBaseCRUD();
	public static JDBCConnection jdbconn = new JDBCConnection();

	public static void main(String[] args) {
		conf.set("fs.default.name", "hdfs://192.168.10.10:9000");
		try {
			fw = new FileWriter("src/ParserData/ParseLog.txt", true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		while (true) {
			dirString = "/usr/data/crawler-01436457627830";
			webSiteString = null;
			keyString = null;
			HBaseCRUD hbase = new HBaseCRUD();
			ResultScanner rs = null;
			System.out.println("loop over");
			
		/////export youku data from hbase
			Date d = new Date();
//			if(d.getHours() < 4) {
//				ParserData.YoukuParserData.ImportInfo.export();
//				ParserData.YoukuParserData.ImportInfoVideo_v2.export();
//			}
			try {
//				rs = hbase.queryAll(SysParams.parseTable_Hbase);
//				Iterator<Result> ite = rs.iterator();
//				while (ite.hasNext()) {
//					Result r = ite.next();
//					byte[] key = r.getRow();
//					if (key != null)
//						keyString = new String(key, "utf-8");
//
//					byte[] dir = r.getValue("C".getBytes(), "dir".getBytes());
//					if (dir != null) {
//						dirString = new String(dir, "utf-8");
//					} else {
//						System.out.println(keyString + " Dir Exception");
//						try {
//							hbase.deleteRow(SysParams.parseTable_Hbase, keyString);
//						} catch (IOException e) {
//							e.printStackTrace();
//
//						}
//						continue;
//					}
					//dirString = "/crawldatas/crawler-81433489147182";
					//56 debug
					//if(dirString.indexOf("81433489147182") < 0)  continue;
					
					System.out.println("parse begin------" + dirString
							+ "-------------------");

					try {
						fw.write("parse" + dirString + " begin at :"
								+ new Date().toString() + "\n");
						fw.flush();
					//	insertLog("解析大文件：" + dirString + "开始", "1");
					} catch (IOException e1) {
						e1.printStackTrace();
					}

					
					
					parse();

					System.out.println("parse end------" + dirString
							+ "-------------------");

					try {
						fw.write("parse end at :" + new Date().toString()
								+ "\n");
						fw.flush();
						insertLog("解析大文件：" + dirString + "结束", "1");
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				//	break;
			//	}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
					rs = null;
				}
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
		}

	}
	public static void parse() throws FileNotFoundException {
	//	createTable();
		parseData();
//		HDFSCrudImpl hdfs = new HDFSCrudImpl();
//		hdfs.delFile(dirString);
//		try {
//			hbase.deleteRow(SysParams.parseTable_Hbase, keyString);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
	}

	public static void createTable() {
		long timestamp = 0;
		Date now = new Date();
		long timeNow = now.getTime() / 1000;
		if (timeNow + 86400 * 3 >= timestamp) {
			timestamp = timeNow + 86400 * 3;
			JDBCConnection conn = new JDBCConnection();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String sd = sdf.format(new Date(Long.parseLong(timestamp
					+ "000")));
			ExportEveryDay.createTable("moviedynamic" + sd, conn);
			ExportEveryDay.createvideoTable("videodynamic" + sd, conn);
			conn.closeConn();
		}
	}
	private static void parseData() throws FileNotFoundException {
		HBaseCRUD.i = 0;
		FileSystem hdfs = null;
//		try {
//			hdfs = FileSystem.getNamed(dirString, conf);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//			return;
//		}
//		FSDataInputStream inputStream = null;
		BufferedReader buff = null;
//		Path dst = new Path(dirString);
//		try {
//			inputStream = hdfs.open(dst);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//			return;
//		}
		File file = new File(dirString);
		InputStream inputStream = new FileInputStream(file);
		buff = new BufferedReader(new InputStreamReader(inputStream));

		ArrayList<HDFSParseThread> pool = new ArrayList<HDFSParseThread>();
		HDFSParseThread.pool = pool;
		HDFSParseThread.br = buff;
		for (int i = 0; i < threadCount; i++) {
			HDFSParseThread crawlerthread = new HDFSParseThread();
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
					try {
						if (inputStream != null)
							inputStream.close();
						if (buff != null)
							buff.close();
					} catch (IOException e) {

						e.printStackTrace();
					}
					break;
				}
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public static void insertLog(String content, String level) {
		JDBCConnection jdbconn = new JDBCConnection();
		String sql = "insert into Log (machine,level,time,content,website,manager)values('192.168.0.119:crawler2.0-13',"
				+ level + "," + "now()" + ",'" + content + "','All web','ALL');";
		jdbconn.update(sql);
		jdbconn.closeConn();
	}
	
}
