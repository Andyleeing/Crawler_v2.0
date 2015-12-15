package ParserData.YoukuParserData;


import java.text.SimpleDateFormat;
import java.util.Date;

import Utils.JDBCConnection;

public class ExportEveryDay {

	public static Date exportday;

	public static void main(String args[]) {
		long timeNow = 1420041000;
		JDBCConnection conn = new JDBCConnection();
		for(int i = 0;i < 200;i++) {
			SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
			String sd = sdf.format(new Date(Long.parseLong(timeNow +"000")));
			ExportEveryDay.createvideoTable("videodynamic" + sd, conn);
			ExportEveryDay.createTable("moviedynamic" + sd, conn);
			timeNow += 86400;
		}
	}
	
	public static void createvideoTable(String tablename, JDBCConnection conn) {
		
		String sql = "create table " +tablename+"( id int(11) primary key auto_increment, rowkey varchar(200), inforowkey varchar(200), playrowkey varchar(200), website varchar(10), flag int(11), timestamp int(11), up int(11), down int(11), sumplaycount int(13), comment int(11), collect int(11), outside int(11),reference int(11), INDEX (`rowkey`),INDEX (`playrowkey`))";
		System.out.println(sql);
		String exist = "SHOW TABLES LIKE '"+ tablename+"'";
		if(conn.tableExist(exist) != null) {
			System.out.println("table "+tablename+" exist");
			return;
		}
		try {
			System.out.println(conn.update(sql));
		} catch (Exception e) {

		}
	}

	public static void createTable(String tablename, JDBCConnection conn) {
		String sql = "CREATE TABLE `"
				+ tablename
				+ "` ("
				+ "`rowkey` varchar(80) CHARACTER SET latin1 DEFAULT NULL,"
				+ "`sumPlayCount` int(13) DEFAULT NULL,"
				+ "`up` int(11) DEFAULT NULL,"
				+ "`down` int(11) DEFAULT NULL,"
				+ "`peopleNum` int(11) DEFAULT NULL,"
				+ "`score` double DEFAULT NULL,"
				+ "`zhishu` double DEFAULT NULL,"
				+ "`flag` int(11) DEFAULT NULL,"
				+ "`comment` int(11) DEFAULT NULL,"
				+ "`todayPlayCount` int(11) DEFAULT NULL,"
				+ "`man` double DEFAULT NULL,"
				+ "`women` double DEFAULT NULL,"
				+ "`quoteCount` int(11) DEFAULT NULL,"
				+ "`timestamp` int(11) DEFAULT NULL,"
				+ "`movieCode` varchar(50) CHARACTER SET latin1 DEFAULT NULL,"
				+ " `movieName` varchar(100) DEFAULT NULL,"
				+ " `free` int(1) DEFAULT 1,"
				+ "`age0` double DEFAULT NULL,"
				+ "`age1` double DEFAULT NULL,"
				+ "`age2` double DEFAULT NULL,"
				+ "`age3` double DEFAULT NULL,"
				+ "`area350000` int(11) DEFAULT NULL,"
				+ "`area360000` int(11) DEFAULT NULL,"
				+ "`area150000` int(11) DEFAULT NULL,"
				+ "`area330000` int(11) DEFAULT NULL,"
				+ "`area340000` int(11) DEFAULT NULL,"
				+ "`area130000` int(11) DEFAULT NULL,"
				+ "`area140000` int(11) DEFAULT NULL,"
				+ "`area110000` int(11) DEFAULT NULL,"
				+ "`area370000` int(11) DEFAULT NULL,"
				+ "`area120000` int(11) DEFAULT NULL,"
				+ "`area210000` int(11) DEFAULT NULL,"
				+ "`area430000` int(11) DEFAULT NULL,"
				+ "`area420000` int(11) DEFAULT NULL,"
				+ "`area410000` int(11) DEFAULT NULL,"
				+ "`area640000` int(11) DEFAULT NULL,"
				+ "`area820000` int(11) DEFAULT NULL,"
				+ "`area650000` int(11) DEFAULT NULL,"
				+ "`area620000` int(11) DEFAULT NULL,"
				+ "`area630000` int(11) DEFAULT NULL,"
				+ "`area810000` int(11) DEFAULT NULL,"
				+ "`area610000` int(11) DEFAULT NULL,"
				+ "`area450000` int(11) DEFAULT NULL,"
				+ "`area440000` int(11) DEFAULT NULL,"
				+ "`area460000` int(11) DEFAULT NULL,"
				+ "`area220000` int(11) DEFAULT NULL,"
				+ "`area230000` int(11) DEFAULT NULL,"
				+ "`area320000` int(11) DEFAULT NULL,"
				+ "`area310000` int(11) DEFAULT NULL,"
				+ "`area510000` int(11) DEFAULT NULL,"
				+ "`area520000` int(11) DEFAULT NULL,"
				+ "`area710000` int(11) DEFAULT NULL,"
				+ "`area530000` int(11) DEFAULT NULL,"
				+ "`area540000` int(11) DEFAULT NULL,"
				+ "`area500000` int(11) DEFAULT NULL,"
				+ "`id` int(11) NOT NULL AUTO_INCREMENT,"
				+ "`occ0` double DEFAULT NULL,"
				+ "`occ1` double DEFAULT NULL,"
				+ "`occ2` double DEFAULT NULL,"
				+ "`occ3` double DEFAULT NULL,"
				+ "`area440300` int(11) DEFAULT NULL,"
				+ "`area440100` int(11) DEFAULT NULL,"
				+ "`website` varchar(10) DEFAULT NULL,"
				+ "`reference` int(11) DEFAULT NULL,"
				+ "`category` varchar(20) DEFAULT NULL,"
				+ "PRIMARY KEY (`id`),"
				+ "INDEX (`rowkey`)"
				+ ") ENGINE=MyISAM AUTO_INCREMENT=6049019 DEFAULT CHARSET=utf8;";
		System.out.println(sql);
		String exist = "SHOW TABLES LIKE '"+ tablename+"'";
		if(conn.tableExist(exist) != null) {
			System.out.println("table "+tablename+" exist");
			return;
		}
		try {
			System.out.println(conn.update(sql));
		} catch (Exception e) {

		}

	}
}