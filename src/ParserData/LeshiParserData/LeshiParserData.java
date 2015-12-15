package ParserData.LeshiParserData;

import hbase.HBaseCRUD;
import Utils.JDBCConnection;

public class LeshiParserData {
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	
	public LeshiParserData(HBaseCRUD hbase, JDBCConnection jdbconn){
		this.hbase=hbase;
		this.jdbconn=jdbconn;
	}
	
	public void resolveData(String line1,String line2){
		int TypeEnd = line1.indexOf("@date");
		int timeEnd=line1.indexOf("dateend");
		String time=line1.substring(TypeEnd+6,timeEnd);
		String type = line1.substring(12, TypeEnd - 1);
		int RowkeyBeg = line1.indexOf(" @rowkey");
		String rowkey = line1.substring(RowkeyBeg + 9);

		try {
			if (type.equals("vt")) {
				TVparse tvparse = new TVparse(hbase,jdbconn);
				tvparse.parser(line2, rowkey, "电视剧", time);
			} else if (type.equals("movie")) {
				movieParse movparse = new movieParse(hbase,jdbconn);
				movparse.parser(line2, rowkey, "电影", time);
			} else if (type.equals("cimoc")) {
				animeParse aniparse = new animeParse(hbase,jdbconn);
				aniparse.parser(line2, rowkey, "动漫", time);
			} else if (type.equals("iygnoz")) {
				zongyiParse zongyiparse = new zongyiParse(hbase,jdbconn);
				zongyiparse.parser(line2, rowkey, "综艺", time);
			}
		} catch (Exception e) {
			
		}
	}
}
