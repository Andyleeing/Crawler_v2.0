package URLcrawler.Iqiyi;

import hbase.HBaseCRUD;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.ArrayList;  
import java.util.Date;  

public class TvDmItm {
	public static  HBaseCRUD   hbase=new HBaseCRUD();
	public static int threadCount = 30;
	public static void m() throws Exception {
		        String  url=null;
		        String parentid=null;
				 ResultScanner rs =hbase.queryAll("iqiyiListAll");
					Iterator<Result> ite = rs.iterator();
					  String[] colfams = {"C"};
					  String[] quals = {"url"};
					  String[] values={""};
					  ArrayList<String> tmpurl=new ArrayList<String>();
					while (ite.hasNext()) {
						try {
						String keyString =  "";
						Result r = ite.next();
						byte[] key = r.getRow();
						if(key != null)
						keyString=new String(key,"utf-8");
						tmpurl.add(keyString);
						} catch(IOException e) {
							e.printStackTrace();
					}
					}
					rs.close();
					rs=null;
					ArrayList<IqiyiThread> pool = new ArrayList<IqiyiThread>();
					Date d=new Date();
					IqiyiThread.urlList =tmpurl;
					for(int i = 0;i < threadCount;i++) {
					  IqiyiThread downthread = new IqiyiThread(d.getTime(),d.toString(),1,pool);
						
						pool.add(downthread);
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						new Thread(downthread).start();
					}
					int count=1;
					while(true) {
						try {
							Thread.sleep(60000);
						} catch(InterruptedException e) {
							e.printStackTrace();
						}
						synchronized(pool) {
							if(pool.size()<=0) {
								break;
							}
						}
						count++;
						if(count==30) {
							break;
						}
					}
					pool=null;		    
	}
	}
	           


