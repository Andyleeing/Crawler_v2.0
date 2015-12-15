package ParserData.SohuParserData;

import hbase.HBaseCRUD;
import ParserData.Iqiyi.parser;
import Utils.JDBCConnection;

public class SohuParserData {
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	public SohuParserData(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}
	
	
	
	public void resolveData(String attribute, String content) {
		SohuParser parser = new SohuParser(hbase,jdbconn);
		if(attribute.equals("") || content.equals(""))return ;
		attribute=attribute.substring(5);
		String[] splits = attribute.split("@@");
		if(splits.length<5)return;   //为什么判断长度？
		if(splits[2].equals("info")) {
			if(splits[5].indexOf("dianying")>=0) {
				parser.dyInfoParser(splits,content,jdbconn);  
			}
			else if(splits[5].indexOf("dongman")>=0||splits[5].indexOf("yingshi")>=0){
				parser.ysdmInfoParser(splits, content,jdbconn);
			}else if(splits[5].indexOf("zongyi")>=0){
				parser.zongyiInfoParser(splits, content,jdbconn);
			}
		} else if(splits[2].equals("play")) {
			if(splits[5].indexOf("dianying")>=0) {
				parser.dyPlayParser(splits,content,jdbconn);//--passs
			} else if(splits[5].indexOf("zongyi")>=0||splits[5].indexOf("yingshi")>=0||splits[5].indexOf("dongman")>=0) {
				parser.ysdmPlayParser(splits,content,jdbconn);//--pass
			}else if((splits[5].indexOf("paid")>=0)){
				parser.getPaidParser(splits, content,jdbconn);
			}
	}
	}
}

