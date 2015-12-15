package ParserData;

import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

import ParserData.Iqiyi.IqiyiParse;
import ParserData.LeshiParserData.LeshiParserData;
import ParserData.SohuParserData.SohuParserData;
import ParserData.TencentParserData.TencentParserData;
import ParserData.XunleiParserData.XunleiParserData;
import ParserData.YoukuParserData.YoukuParserData;
import ParserData.syl56ParserData.syl56ParserData;
import Utils.JDBCConnection;

public class HDFSParseThread implements Runnable {

	public static BufferedReader br;
	public static ArrayList<HDFSParseThread> pool;
	public HBaseCRUD hbase = new HBaseCRUD();
	public JDBCConnection jdbconn = new JDBCConnection();
	LeshiParserData ls = new LeshiParserData(hbase, jdbconn);
	YoukuParserData yk = new YoukuParserData(hbase, jdbconn);
	TencentParserData tc = new TencentParserData(hbase, jdbconn);

	public HDFSParseThread() {

	}

	public void run() {
		while (true) {
			String info = null;
			String content = null;
			synchronized (br) {
				try {
					info = br.readLine();
					content = br.readLine();

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (content == null) {
					synchronized (pool) {
						pool.remove(this);
						try {
							hbase.commitPuts();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						yk = null;
						ls = null;
						tc = null;
						break;
					}
				}
			}
			if (content.equals("") || info.equals(""))
				continue;
			int index = info.indexOf(" ");
			if (index > 0) {
				String website = info.substring(0, index);
				long ms = System.currentTimeMillis();
				if (website.equals("xunlei")) {
					XunleiParserData ex = new XunleiParserData(hbase, jdbconn);
					ex.resolveData(content);
					ex = null;
				} else if (website.equals("leshi")) {
					ls.resolveData(info, content);
				} else if (website.equals("youku")) {
					yk.resolveData(info, content);
				} else if (website.equals("tencent")) {
					tc.resolveDate(info, content);
				} else if (website.equals("Iqiyi")) {
					IqiyiParse ex = new IqiyiParse(hbase, jdbconn);
					ex.iyparse(info, content);
					ex = null;
				} else if (website.equals("56")) {
					syl56ParserData ex = new syl56ParserData(hbase, jdbconn);
					ex.resolveData(info, content);
					ex = null;
				}
				// sohu
				 else if (website.equals("sohu")) {
				 SohuParserData sh = new SohuParserData(hbase,jdbconn);
				 sh.resolveData(info, content);
				 sh = null;
				 }

				//

				else {
					System.out.println("error");
				}
				long time = (System.currentTimeMillis() - ms);

				System.out.println(website + ":" + time);

			}

		}

	}

}
