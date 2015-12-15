package URLcrawler.Iqiyi;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import Utils.TextValue;
import hbase.HBaseCRUD;

public class iqiyi {
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static String df = "";
	

	public void Iqiyi() {
		try {

			Date date = new Date();
			SimpleDateFormat form = new SimpleDateFormat("yyyyMMdd");
			df = form.format(date);
			long timeNow = date.getTime() / 1000;
			long timestamp = 1422115200;
			if (timeNow + 86400 * 3 >= timestamp) {
				timestamp = timeNow + 86400 * 3;
				JDBCConnection conn = new JDBCConnection();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				String sd = sdf.format(new Date(Long.parseLong(timestamp
						+ "000")));
				createTable("urls" + sd, conn);
				conn.closeConn();
			}

			

			Item.Moviem();
			Item.MvHm();
			Item.DmInm();
			Item.TvInm();
			Item.ZyInm();
			TvDmItm.m();

			JDBCConnection jdbconn = new JDBCConnection();
			HBaseCRUD crud = new HBaseCRUD();
			ResultScanner rs = null;
			try {
				rs = crud.queryAll("iqiyiListAll");
			} catch (IOException e) {
			}
			Iterator<Result> ite = rs.iterator();

			while (ite.hasNext()) { // iqiyiList export to MySqL.
				String keyString = "";
				Result r = ite.next();
				byte[] key = r.getRow();
				if (key == null) {
					continue;
				}
				keyString = new String(key, "utf-8");
				IqiyiInfoImport(keyString, r, jdbconn);
			}

			

			System.out.println(" Iqiyi loop over at " + new Date().toString());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void IqiyiInfoImport(String keyString, Result r,
			JDBCConnection jdbconn) {

		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = keyString + "+iy";
		values.add(rowkeytv);

		TextValue url = new TextValue();
		url.text = "url";
		url.value = keyString;
		values.add(url);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "iy";
		values.add(namewebsite);
		jdbconn.insert(values, "urls" + df);

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

}
