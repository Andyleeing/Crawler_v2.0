package URLcrawler;

import hbase.HBaseCRUD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;



public class test {
	public static void main(String args[]) {
	
		int count = 0;
		HBaseCRUD hbase = new HBaseCRUD();
		ResultScanner ykrs = null;
		ResultScanner iyrs = null;
		ResultScanner lsrs = null;
		ResultScanner xlrs = null;
		ResultScanner txrs = null;
		ResultScanner _56rs = null;

		try {
		ykrs = hbase.queryAll("infoplayall");
			iyrs = hbase.queryAll("iqiyiListAll");
			lsrs = hbase.queryAll("leurlnew");
			xlrs = hbase.queryAll("xunleinew");
			txrs = hbase.queryAll("tencentnew");
			_56rs = hbase.queryAll("url56new");
		Iterator<Result> iteyk = ykrs.iterator();
			Iterator<Result> iteiy = iyrs.iterator();
			Iterator<Result> itels = lsrs.iterator();
			Iterator<Result> itexl = xlrs.iterator();
			Iterator<Result> itetx = txrs.iterator();
			Iterator<Result> ite56 = _56rs.iterator();
			ArrayList<ResultScanner> rss = new ArrayList<ResultScanner>();
			ArrayList<Iterator<Result>> ites = new ArrayList<Iterator<Result>>();
			rss.add(ykrs);
			rss.add(iyrs);
			rss.add(lsrs);
			rss.add(xlrs);
			rss.add(txrs);
			rss.add(_56rs);
			ites.add(iteyk);
			ites.add(iteiy);
			ites.add(itels);
			ites.add(itexl);
			ites.add(itetx);
			ites.add(ite56);
			
			int indexRs = 0;
			int num=1;
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
					
					String nu=String.format("%03d",num);
				   String[] rows = null;
				   String[] colfams = null;
					String[] quals = null;
					String[] values = null;
					rows = new String[] { nu+new Date().getTime() + "" };
					// rowkey++;
					Thread.sleep(1);
					colfams = new String[] { "C" };
					quals = new String[] { "url" };
					values = new String[] { urlString };
					try {
						hbase.putRows("AllUrl", rows, colfams, quals, values);
						num++;
						if(num>50) 
							num=1;
					} catch (Exception e) {
						e.printStackTrace();
					}
					rows = null;
					colfams = null;
					quals = null;
					values = null;
					indexRs++;
					if(indexRs>=rss.size())
						indexRs=0;
			

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
